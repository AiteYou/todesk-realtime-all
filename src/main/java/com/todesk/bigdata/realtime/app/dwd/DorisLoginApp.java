package com.todesk.bigdata.realtime.app.dwd;

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
 * Date : 2023/7/28 15:26
 * Desc :
 */
public class DorisLoginApp {
    private static final Map<OutputTag<String>, String> tagTableMapping = new HashMap<>();
    static {
        tagTableMapping.put(new OutputTag<String>("phoneLogin"){}, "todesk.dwd_phone_login");
        tagTableMapping.put(new OutputTag<String>("autoLogin"){}, "todesk.dwd_auto_login");
        //tagTableMapping.put(new OutputTag<String>("errorlog"){}, "todesk.dwd_login_error");
        tagTableMapping.put(new OutputTag<String>("qrcodeLogin"){}, "todesk.dwd_qrcode_login");
        tagTableMapping.put(new OutputTag<String>("wechatLogin"){}, "todesk.dwd_wechat_login");
        tagTableMapping.put(new OutputTag<String>("user_login"){}, "todesk.dwd_user_login");
        tagTableMapping.put(new OutputTag<String>("user_logout"){}, "todesk.dwd_user_logout");
        tagTableMapping.put(new OutputTag<String>("request"){}, "todesk.dwd_web_request");
        tagTableMapping.put(new OutputTag<String>("sendsms"){}, "todesk.dwd_send_sms");
        tagTableMapping.put(new OutputTag<String>("sendemail"){}, "todesk.dwd_send_email");
        tagTableMapping.put(new OutputTag<String>("http"){}, "todesk.dwd_message_center");
        tagTableMapping.put(new OutputTag<String>("DuplexMessage"){}, "todesk.dwd_duplex_message");
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);
        //env.disableOperatorChaining();

        String topic = "doris_dwd_login_log";
        String groupId = "kafka_to_doris";

        //FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        FlinkKafkaConsumer<String> kafkaSource = (FlinkKafkaConsumer<String>) KafkaUtil.getKafkaSource(topic, groupId).setStartFromLatest();
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource).setParallelism(3);
        SingleOutputStreamOperator<String> resultDS = kafkaDS.process(new MyProcessFunction.LogTypeFlagFunction()).setParallelism(3);

        List<OutputTag<String>> outputTags = new ArrayList<>(tagTableMapping.keySet());

        for (OutputTag<String> tag : outputTags) {
            resultDS.getSideOutput(tag).addSink(DorisUtil.getDorisSink(tagTableMapping.get(tag),10)).name(tag.getId()).setParallelism(1);
        }

        env.execute("DorisLoginApp");
    }
}
