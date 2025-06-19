package com.todesk.bigdata.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.todesk.bigdata.realtime.function.MyMapFunction;
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
 * Date : 2023/7/12 17:16
 * Desc :
 */
public class DorisRiskManagementApp {
    private static final Map<OutputTag<String>, String> tagTableMapping = new HashMap<>();
    static {
        tagTableMapping.put(new OutputTag<String>("newconnect_fail"){}, "todesk.dwd_newconnect_fail");
        tagTableMapping.put(new OutputTag<String>("wrongpass_locktime"){}, "todesk.dwd_wrongpass_locktime");
        tagTableMapping.put(new OutputTag<String>("access_check_fail"){}, "todesk.dwd_access_check_fail");
        tagTableMapping.put(new OutputTag<String>("clean_whitelist"){}, "todesk.dwd_clean_whitelist");
        tagTableMapping.put(new OutputTag<String>("add_black_list"){}, "todesk.dwd_add_black_list");
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);

        String topic = "doris_dwd_risk_management_log";
        String groupId = "kafka_to_doris";

        //FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        FlinkKafkaConsumer<String> kafkaSource = (FlinkKafkaConsumer<String>) KafkaUtil.getKafkaSource(topic, groupId).setStartFromLatest();
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource).setParallelism(3);
        SingleOutputStreamOperator<String> resultDS = kafkaDS
                .filter(data->! JSON.parseObject(data).containsKey("duid") || JSON.parseObject(data).getString("duid").length()<16)
                .process(new MyProcessFunction.LogTypeFlagFunction()).setParallelism(3);

        List<OutputTag<String>> outputTags = new ArrayList<>(tagTableMapping.keySet());

        for (OutputTag<String> tag : outputTags) {
            resultDS.getSideOutput(tag).addSink(DorisUtil.getDorisSink(tagTableMapping.get(tag),10)).name(tag.getId()).setParallelism(1);
        }

        env.execute("DorisRiskManagementApp");
    }
}