package com.todesk.bigdata.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.utils.EnvConfig;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * Author : Liuchao
 * Date : 2023/3/1 11:10
 * Desc :
 */
public class AllLogAppTest {
    private static final Map<String, String> tagTopicMapping = new HashMap<String, String>();
    static {
        tagTopicMapping.put("dirty", "ods_dirty_data");
        tagTopicMapping.put("newconnect", "dwd_newconnect_log");
        tagTopicMapping.put("handshake_success", "dwd_handshake_success_log");
        tagTopicMapping.put("user_login", "dwd_user_login_log");
        tagTopicMapping.put("client_connect", "dwd_client_connect_log");
        tagTopicMapping.put("client_closed", "dwd_client_closed_log");
        tagTopicMapping.put("phoneLogin", "dwd_client_login_log");
        tagTopicMapping.put("errorlog", "dwd_client_login_log");
        tagTopicMapping.put("autoLogin", "dwd_client_login_log");
        tagTopicMapping.put("configcenter", "dwd_client_config_center_log");
        tagTopicMapping.put("sendemail", "dwd_client_send_log");
        tagTopicMapping.put("sendsms", "dwd_client_send_log");
        tagTopicMapping.put("link_tech", "dwd_link_tech_log");
        tagTopicMapping.put("setting_tech", "dwd_setting_log");
        tagTopicMapping.put("remote_link_process", "dwd_remote_link_process_log");
        tagTopicMapping.put("account", "dwd_click_login_log");
        tagTopicMapping.put("newuser", "new_user_log");
    }

    public static void main(String[] args) throws Exception {
        /*Set<String> strings = tagTopicMapping.keySet();
        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()){
            String next = iterator.next();
            String value = tagTopicMapping.get(next);
            System.out.println(next+':'+value);
        }
        System.out.println(tagTopicMapping.size());*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);
        env.disableOperatorChaining();

        String topic = "test1";
        String groupId = "test1_group";
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource).setParallelism(6);

        SingleOutputStreamOperator<String> separatedStream = kafkaDS.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<String> out) throws Exception {
                        JSONObject jsonObj = null;
                        try {
                            jsonObj = JSONObject.parseObject(jsonStr);
                            String logtypeStr = jsonObj.getString("logtype");
                            String labtypeStr = jsonObj.getString("labtype");
                            String eventsStr = jsonObj.getString("events");
                            String moduleStr = jsonObj.getString("module");
                            String pageStr = jsonObj.getString("page");
                            if (logtypeStr != null) {
                                switch (logtypeStr) {
                                    case "newconnect":
                                    case "handshake_success":
                                    case "user_login":
                                    case "client_connect":
                                    case "client_closed":
                                    case "newuser":
                                    case "phoneLogin":
                                    case "errorlog":
                                    case "autoLogin":
                                    case "sendemail":
                                    case "sendsms":
                                        ctx.output(new OutputTag<String>(tagTopicMapping.get(logtypeStr)) {
                                        }, jsonStr);
                                        break;
                                    case "config_center":
                                        if ("configcenter".equals(labtypeStr)) {
                                            ctx.output(new OutputTag<String>(tagTopicMapping.get(labtypeStr)) {
                                            }, jsonStr);
                                        }
                                        break;
                                    case "tuevent":
                                        if ( "link_tech".equals(eventsStr) || "setting_tech".equals(eventsStr) || "remote_link_process".equals(eventsStr) ){
                                                ctx.output(new OutputTag<String>(tagTopicMapping.get(eventsStr)) {
                                                }, jsonStr);
                                        }
                                        break;
                                    case "click":
                                        if ("account".equals(moduleStr) && "signin".equals(pageStr)) {
                                            ctx.output(new OutputTag<String>(tagTopicMapping.get(moduleStr)) {
                                            }, jsonStr);
                                        }
                                    default:
                                        out.collect(jsonStr);
                                        break;
                                }

                            }

                        } catch (Exception e) {
                            ctx.output(new OutputTag<String>(tagTopicMapping.get("dirty")) {
                            }, jsonStr);
                        }
                    }
                }
        );




    }

}
