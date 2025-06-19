package com.todesk.bigdata.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.function.MyMapFunction;
import com.todesk.bigdata.realtime.utils.DorisUtil;
import com.todesk.bigdata.realtime.utils.EnvConfig;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * Author : Liuchao
 * Date : 2023/7/5 15:54
 * Desc :
 */
public class DorisAllLogApp {
    private static final Map<OutputTag<String>, String> tagTopicMapping = new LinkedHashMap<>();
    static {
        tagTopicMapping.put(new OutputTag<String>("dirty"){}, "ods_dirty_data");
        tagTopicMapping.put(new OutputTag<String>("backend"){}, "doris_dwd_backend_log");
        tagTopicMapping.put(new OutputTag<String>("risk"){}, "doris_dwd_risk_management_log");
        tagTopicMapping.put(new OutputTag<String>("login"){}, "doris_dwd_login_log");
        tagTopicMapping.put(new OutputTag<String>("config_center"){}, "doris_dwd_config_center_log");
        tagTopicMapping.put(new OutputTag<String>("client_con"){}, "doris_dwd_client_con_log");
        tagTopicMapping.put(new OutputTag<String>("click"){}, "doris_dwd_click_log");
        tagTopicMapping.put(new OutputTag<String>("event"){}, "doris_dwd_event_log");
        tagTopicMapping.put(new OutputTag<String>("tevent"){}, "doris_dwd_tevent_log");
        tagTopicMapping.put(new OutputTag<String>("tuevent"){}, "doris_dwd_tuevent_log");
        tagTopicMapping.put(new OutputTag<String>("licode"){}, "doris_dwd_licode_log");
    }

    public static void main(String[] args) throws Exception {

       /* List<OutputTag<String>> outputTags = new ArrayList<>(tagTopicMapping.keySet());

          Set<String> strings = tagTopicMapping.keySet();
        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()){
            String next = iterator.next();
            String value = tagTopicMapping.get(next);
            System.out.println(next+':'+value);
        }
        System.out.println(tagTopicMapping.size());*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);

        String topic = "test1";
        String groupId = "kafka_to_doris";

        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        //FlinkKafkaConsumer<String> kafkaSource1 = (FlinkKafkaConsumer<String>) KafkaUtil.getKafkaSource(topic, groupId).setStartFromLatest();
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource).setParallelism(6);

        List<OutputTag<String>> outputTags = new ArrayList<>(tagTopicMapping.keySet());

        SingleOutputStreamOperator<String> separatedStream = kafkaDS.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<String> out) throws Exception {
                        JSONObject jsonObj = null;
                        try {
                            jsonObj = JSONObject.parseObject(jsonStr);
                            String logtypeStr = jsonObj.getString("logtype");
                            if (logtypeStr != null) {
                                switch (logtypeStr) {
                                    case "newconnect":
                                    case "handshake_success":
                                    case "authentication_success":
                                    case "connection_stat":
                                    case "add_device":
                                    case "delete_device":
                                    case "add_group":
                                    case "delete_group":
                                    case "connection_closed":
                                    case "controller_not_present":
                                    case "presenter_not_present":
                                    case "newuser":
                                    case "del_fserver":
                                    case "Connect_Limit_Sline":
                                    case "passtimelock":
                                    case "newfileconnect":
                                    case "newfileconnect_fail":
                                    case "send_cmd":
                                    case "send_cmd_fail":
                                    case "control_remote_camera":
                                    case "control_remote_camera_fail":
                                    case "connect_limit":
                                    case "freepassword":
                                    case "ban_P1":
                                        ctx.output(outputTags.get(1), jsonStr);
                                        break;
                                    case "newconnect_fail":
                                    case "wrongpass_locktime":
                                    case "access_check_fail":
                                    case "clean_whitelist":
                                    case "add_black_list":
                                        ctx.output(outputTags.get(2), jsonStr);
                                        break;
                                    case "phoneLogin":
                                    case "autoLogin":
                                    case "errorlog":
                                    case "qrcodeLogin":
                                    case "wechatLogin":
                                    case "user_login":
                                    case "user_logout":
                                    case "request":
                                    case "sendsms":
                                    case "sendemail":
                                    case "http":
                                    case "DuplexMessage":
                                        ctx.output(outputTags.get(3), jsonStr);
                                        break;
                                    case "config_center":
                                        ctx.output(outputTags.get(4), jsonStr);
                                        break;
                                    case "client_connect":
                                    case "client_closed":
                                        ctx.output(outputTags.get(5), jsonStr);
                                        break;
                                    case "click":
                                        ctx.output(outputTags.get(6), jsonStr);
                                        break;
                                    case "event":
                                        ctx.output(outputTags.get(7), jsonStr);
                                        break;
                                    case "tevent":
                                        ctx.output(outputTags.get(8), jsonStr);
                                        break;
                                    case "tuevent":
                                        ctx.output(outputTags.get(9), jsonStr);
                                        break;
                                    case "publish_ready" :
                                    case "publish_start" :
                                    case "subscribe_ready" :
                                    case "subscribe_sfu" :
                                    case "publish_erizo" :
                                    case "publish_initializing" :
                                    case "subscribe_start" :
                                    case "subscribe_p2p" :
                                    case "subscribe_end" :
                                    case "publish_end" :
                                    case "WebConnection" :
                                    case "media_statistic" :
                                    case "firstPacketReceive" :
                                    case "addSubscriber_ready" :
                                    case "addPublisher_ready" :
                                    case "socket_disconnected" :
                                    case "licode_newconnection" :
                                    case "sdk_report" :
                                    case "licode_reconnection" :
                                    case "licode_connection_stat" :
                                    case "onUnsubscribe" :
                                    case "onUnpublish" :
                                    case "publish_started":
                                    case "publish_failed":
                                    case "publish_timeout":
                                    case "publish_relay":
                                    case "subscribe_unauthorized":
                                    case "publish_Unauthorized":
                                    case "subscribe_publisher-not-found":
                                    case "subscribe_failed":
                                    case "subscribe_initializing":
                                    case "subscribe_started":
                                    case "licode_allconnected":
                                    case "licode_auth_success":
                                    case "addPublisher_initializing":
                                    case "addPublisher_started":
                                    case "addSubscriber_initializing":
                                    case "createOffer":
                                    case "sendOffer":
                                    case "sendAnswer":
                                    case "processConnectionMessage_client-not-found":
                                    case "publish_p2p":
                                        ctx.output(outputTags.get(10), jsonStr);
                                        break;
                                    default:
                                        out.collect(jsonStr);
                                        break;
                                }
                            }
                        } catch (Exception e) {
                            ctx.output(outputTags.get(0), jsonStr);
                        }
                    }
                }
        ).setParallelism(6);

        for (OutputTag<String> tag : outputTags) {
            separatedStream.getSideOutput(tag)
                    .flatMap(new MyMapFunction.PutDateFunction())
                    .addSink(KafkaUtil.getKafkaSink(tagTopicMapping.get(tag)))
                    .name(tag.getId())
                    .setParallelism(3);
        }
        env.execute("DorisAllLogApp");
    }
}
