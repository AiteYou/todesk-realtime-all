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
 * Date : 2023/8/1 18:27
 * Desc :
 */
public class DorisBackendLicodeApp {
    private static final Map<OutputTag<String>, String> tagTableMapping = new HashMap<>();
    static {
        tagTableMapping.put(new OutputTag<String>("publish_ready"){}, "todesk.dwd_backend_licode_publish_ready");
        tagTableMapping.put(new OutputTag<String>("publish_start"){}, "todesk.dwd_backend_licode_publish_start");
        tagTableMapping.put(new OutputTag<String>("subscribe_ready"){}, "todesk.dwd_backend_licode_subscribe_ready");
        tagTableMapping.put(new OutputTag<String>("subscribe_sfu"){}, "todesk.dwd_backend_licode_subscribe_sfu");
        tagTableMapping.put(new OutputTag<String>("publish_erizo"){}, "todesk.dwd_backend_licode_publish_erizo");
        tagTableMapping.put(new OutputTag<String>("publish_initializing"){}, "todesk.dwd_backend_licode_publish_initializing");
        tagTableMapping.put(new OutputTag<String>("subscribe_start"){}, "todesk.dwd_backend_licode_subscribe_start");
        tagTableMapping.put(new OutputTag<String>("subscribe_p2p"){}, "todesk.dwd_backend_licode_subscribe_p2p");
        tagTableMapping.put(new OutputTag<String>("subscribe_end"){}, "todesk.dwd_backend_licode_subscribe_end");
        tagTableMapping.put(new OutputTag<String>("publish_end"){}, "todesk.dwd_backend_licode_publish_end");
        tagTableMapping.put(new OutputTag<String>("WebConnection"){}, "todesk.dwd_backend_licode_web_connection");
        tagTableMapping.put(new OutputTag<String>("media_statistic"){}, "todesk.dwd_media_statistic");
        tagTableMapping.put(new OutputTag<String>("firstPacketReceive"){}, "todesk.dwd_licode_first_packet_receive");
        tagTableMapping.put(new OutputTag<String>("addSubscriber_ready"){}, "todesk.dwd_backend_licode_addsubscriber_ready");
        tagTableMapping.put(new OutputTag<String>("addPublisher_ready"){}, "todesk.dwd_backend_licode_addpublisher_ready");  //todo 记得改为小写
        tagTableMapping.put(new OutputTag<String>("socket_disconnected"){}, "todesk.dwd_backend_licode_socket_disconnected");
        tagTableMapping.put(new OutputTag<String>("licode_newconnection"){}, "todesk.dwd_backend_licode_newconnect");
        tagTableMapping.put(new OutputTag<String>("sdk_report"){}, "todesk.dwd_backend_licode_sdk_report");
        tagTableMapping.put(new OutputTag<String>("licode_reconnection"){}, "todesk.dwd_backend_licode_reconnect");
        tagTableMapping.put(new OutputTag<String>("licode_connection_stat"){}, "todesk.dwd_licode_session_success");
        tagTableMapping.put(new OutputTag<String>("onUnsubscribe"){}, "todesk.dwd_backend_licode_unsubscribe");
        tagTableMapping.put(new OutputTag<String>("onUnpublish"){}, "todesk.dwd_backend_licode_unpublish");
        tagTableMapping.put(new OutputTag<String>("publish_started"){}, "todesk.dwd_backend_licode_publish_started");
        tagTableMapping.put(new OutputTag<String>("publish_failed"){}, "todesk.dwd_backend_licode_publish_failed");
        tagTableMapping.put(new OutputTag<String>("publish_timeout"){}, "todesk.dwd_backend_licode_publish_timeout");
        tagTableMapping.put(new OutputTag<String>("publish_relay"){}, "todesk.dwd_backend_licode_publish_relay");
        tagTableMapping.put(new OutputTag<String>("subscribe_unauthorized"){}, "todesk.dwd_backend_licode_subscribe_unauthorized");
        tagTableMapping.put(new OutputTag<String>("publish_Unauthorized"){}, "todesk.dwd_backend_licode_publish_unauthorized");
        tagTableMapping.put(new OutputTag<String>("subscribe_publisher-not-found"){}, "todesk.dwd_backend_licode_subscribe_publisher_not_found");
        tagTableMapping.put(new OutputTag<String>("subscribe_failed"){}, "todesk.dwd_backend_licode_subscribe_failed");
        tagTableMapping.put(new OutputTag<String>("subscribe_initializing"){}, "todesk.dwd_backend_licode_subscribe_initializing");
        tagTableMapping.put(new OutputTag<String>("subscribe_started"){}, "todesk.dwd_backend_licode_subscribe_started");
        tagTableMapping.put(new OutputTag<String>("licode_allconnected"){}, "todesk.dwd_backend_licode_allconnected");
        tagTableMapping.put(new OutputTag<String>("licode_auth_success"){}, "todesk.dwd_backend_licode_auth_success");
        tagTableMapping.put(new OutputTag<String>("addPublisher_initializing"){}, "todesk.dwd_backend_licode_addpublisher_initializing");
        tagTableMapping.put(new OutputTag<String>("addPublisher_started"){}, "todesk.dwd_backend_licode_addpublisher_started");
        tagTableMapping.put(new OutputTag<String>("addSubscriber_initializing"){}, "todesk.dwd_backend_licode_addsubscriber_initializing");
        tagTableMapping.put(new OutputTag<String>("createOffer"){}, "todesk.dwd_backend_licode_create_offer");
        tagTableMapping.put(new OutputTag<String>("sendOffer"){}, "todesk.dwd_backend_licode_send_offer");
        tagTableMapping.put(new OutputTag<String>("sendAnswer"){}, "todesk.dwd_backend_licode_send_answer");
        tagTableMapping.put(new OutputTag<String>("processConnectionMessage_client-not-found"){}, "todesk.dwd_backend_licode_process_client_not_found");
        tagTableMapping.put(new OutputTag<String>("publish_p2p"){}, "todesk.dwd_backend_licode_publish_p2p");
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);
        //env.disableOperatorChaining();

        String topic = "doris_dwd_licode_log";
        String groupId = "kafka_to_doris";

        //FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        FlinkKafkaConsumer<String> kafkaSource = (FlinkKafkaConsumer<String>) KafkaUtil.getKafkaSource(topic, groupId).setStartFromLatest();
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource).setParallelism(3);
        SingleOutputStreamOperator<String> resultDS = kafkaDS.process(new MyProcessFunction.LogTypeFlagFunction()).setParallelism(3);

        List<OutputTag<String>> outputTags = new ArrayList<>(tagTableMapping.keySet());

        for (OutputTag<String> tag : outputTags) {
            resultDS.getSideOutput(tag).addSink(DorisUtil.getDorisSink(tagTableMapping.get(tag),10)).name(tag.getId()).setParallelism(1);
        }

        env.execute("DorisBackendLicodeApp");
    }
}