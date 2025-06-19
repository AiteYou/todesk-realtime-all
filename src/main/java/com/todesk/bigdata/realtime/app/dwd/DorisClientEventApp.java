package com.todesk.bigdata.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.todesk.bigdata.realtime.function.MyProcessFunction;
import com.todesk.bigdata.realtime.utils.DorisUtil;
import com.todesk.bigdata.realtime.utils.EnvConfig;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author : Liuchao
 * Date : 2023/7/28 16:04
 * Desc :
 */
public class DorisClientEventApp {
    private static final Map<OutputTag<String>, String> tagTableMapping = new HashMap<>();
    static {
        tagTableMapping.put(new OutputTag<String>("event"){}, "todesk.dwd_client_event");
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);
        //env.disableOperatorChaining();

        String topic = "doris_dwd_event_log";
        String groupId = "kafka_to_doris";

        //FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        FlinkKafkaConsumer<String> kafkaSource = (FlinkKafkaConsumer<String>) KafkaUtil.getKafkaSource(topic, groupId).setStartFromLatest();
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource).setParallelism(3);
        SingleOutputStreamOperator<String> resultDS = kafkaDS
                .filter(data->! JSON.parseObject(data).containsKey("ck_url") || JSON.parseObject(data).getString("ck_url").getBytes("UTF-8").length<1048576)
                .process(new MyProcessFunction.LogTypeFlagFunction()).setParallelism(3);

        List<OutputTag<String>> outputTags = new ArrayList<>(tagTableMapping.keySet());

        for (OutputTag<String> tag : outputTags) {
            resultDS.getSideOutput(tag).addSink(DorisUtil.getDorisSink(tagTableMapping.get(tag),10)).name(tag.getId()).setParallelism(3);
        }

        env.execute("DorisClientEventApp");
    }
}
