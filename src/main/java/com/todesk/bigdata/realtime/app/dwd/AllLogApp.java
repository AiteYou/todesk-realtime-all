package com.todesk.bigdata.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;
/**
 * 将不同日志放入不同的主题中
 */


public class AllLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);*/

        //设置并行度，和kafka分区数保持一致
        env.setParallelism(6);

        //TODO 2.设置检查点

        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        //Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        //cancel job时，Checkpoint是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //job失败，重启策略 5分钟内若失败了3次则认为该job失败，重试间隔为10s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)));

        //ck超时
        env.getCheckpointConfig().setCheckpointTimeout(120000L);

        //设置状态后端1.13版本更新了此API
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        //System.setProperty("HADOOP_USER_NAME", "bigdata");

        env.disableOperatorChaining();

        //TODO 3.从kafka ods中获取数据
        String topic = "test1";
        String groupId = "test1_group";


        // 获取kafka消费者对象
            FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);

        // 将kafka数据封装为流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        //TODO 4.脏数据写入topic中

        OutputTag<String> dirtyDataTag = new OutputTag<String>("dirty"){};

        SingleOutputStreamOperator<String> cleanedStream = kafkaDS.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = null;
                try {
                    jsonObject = JSON.parseObject(jsonStr);
                    // 主流输出
                    out.collect(jsonObject.toJSONString());
                } catch (Exception e) {
                    // 侧输出流输出：如果不是JSON，丢到侧输出流中，写入kafka对应topic
                    ctx.output(dirtyDataTag, jsonStr);
                }
            }
        });
        DataStream<String> dirtyStream = cleanedStream.getSideOutput(dirtyDataTag);
        String dirtyTopic = "ods_dirty_data";
        dirtyStream.addSink(KafkaUtil.getKafkaSink(dirtyTopic));

        //这个是分流的
        //TODO 5.转换数据类型
        SingleOutputStreamOperator<JSONObject> jsonObjDS = cleanedStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                return JSON.parseObject(jsonStr);
            }
        });


        // TODO 6.数据分流
        OutputTag<String> backendNewConnectTag = new OutputTag<String>("backendNewConnect"){};
        OutputTag<String> handshakeSuccessTag = new OutputTag<String>("handshakeSuccess"){};
        OutputTag<String> userLoginTag = new OutputTag<String>("userLogin"){};
        OutputTag<String> clientConnectTag = new OutputTag<String>("clientConnect"){};
        OutputTag<String> clientClosedTag = new OutputTag<String>("clientClosed"){};
        OutputTag<String> clientLoginTag = new OutputTag<String>("clientLogin"){};
        OutputTag<String> clientConfigCenterTag = new OutputTag<String>("clientConfigCenter"){};
        OutputTag<String> clientSendTag = new OutputTag<String>("clientSend"){};
        OutputTag<String> linkTechTag = new OutputTag<String>("linkTech"){};
        OutputTag<String> settingTag = new OutputTag<String>("setting_tech"){};
        OutputTag<String> linkProTag = new OutputTag<String>("remote_link_process"){};
        OutputTag<String> clickLoginTag = new OutputTag<String>("click_login"){};
        OutputTag<String> newUserTag = new OutputTag<String>("new_user"){};

        SingleOutputStreamOperator<String> separatedStream = jsonObjDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                String logtypeStr = jsonObj.getString("logtype");
                String labtypeStr = jsonObj.getString("labtype");
                String eventsStr = jsonObj.getString("events");
                String moduleStr = jsonObj.getString("module");
                String pageStr = jsonObj.getString("page");
                /*JSONObject logtype = jsonObj.getJSONObject("logtype");
                String logtypeStr = logtype.toJSONString();*/
                String jsonString = jsonObj.toJSONString();

                if ("newconnect".equals(logtypeStr)) {
                    ctx.output(backendNewConnectTag, jsonString);
                } else if ("handshake_success".equals(logtypeStr)) {
                    ctx.output(handshakeSuccessTag, jsonString);
                } else if ("user_login".equals(logtypeStr)) {
                    ctx.output(userLoginTag, jsonString);
                } else if ("client_connect".equals(logtypeStr)) {
                    ctx.output(clientConnectTag, jsonString);
                } else if ("client_closed".equals(logtypeStr)) {
                    ctx.output(clientClosedTag, jsonString);
                } else if ("phoneLogin".equals(logtypeStr) || "errorlog".equals(logtypeStr) || "autoLogin".equals(logtypeStr)) {
                    ctx.output(clientLoginTag, jsonString);
                } else if ("config_center".equals(logtypeStr) && "configcenter".equals(labtypeStr)){
                    ctx.output(clientConfigCenterTag,jsonString);
                } else if("sendemail".equals(logtypeStr) || "sendsms".equals(logtypeStr) ){
                    ctx.output(clientSendTag,jsonString);
                } else if("tuevent".equals(logtypeStr) && "link_tech".equals(eventsStr) ){
                    ctx.output(linkTechTag,jsonString);
                } else if("tuevent".equals(logtypeStr) && "setting_tech".equals(eventsStr) ){
                    ctx.output(settingTag,jsonString);
                } else if("tuevent".equals(logtypeStr) && "remote_link_process".equals(eventsStr) ){
                    ctx.output(linkProTag,jsonString);
                } else if("click".equals(logtypeStr) && "account".equals(moduleStr)  && "signin".equals(pageStr) ){
                    ctx.output(clickLoginTag,jsonString);
                } else if("newuser".equals(logtypeStr) ){
                    ctx.output(newUserTag,jsonString);
                } else {
                    out.collect(jsonString);
                }
            }
        });

        DataStream<String> backendNewConnectStream = separatedStream.getSideOutput(backendNewConnectTag);
        DataStream<String> backendHandshakeSuccessStream = separatedStream.getSideOutput(handshakeSuccessTag);
        DataStream<String> userLoginStream = separatedStream.getSideOutput(userLoginTag);
        DataStream<String> clientConnectStream = separatedStream.getSideOutput(clientConnectTag);
        DataStream<String> clientClosedStream = separatedStream.getSideOutput(clientClosedTag);
        DataStream<String> clientLoginStream = separatedStream.getSideOutput(clientLoginTag);
        DataStream<String> clientConfigCenterStream = separatedStream.getSideOutput(clientConfigCenterTag);
        DataStream<String> clientSendStream = separatedStream.getSideOutput(clientSendTag);
        DataStream<String> linkTechStream = separatedStream.getSideOutput(linkTechTag);
        DataStream<String> settingStream = separatedStream.getSideOutput(settingTag);
        DataStream<String> linkProStream = separatedStream.getSideOutput(linkProTag);
        DataStream<String> clickLoginStream = separatedStream.getSideOutput(clickLoginTag);
        DataStream<String> newUserStream = separatedStream.getSideOutput(newUserTag);


        /*dirtyStream.print("dirty>>>");
        separatedStream.print("other>>>");
        backendNewConnectStream.print("backendNewConnectStream>>>");
        backendHandshakeSuccessStream.print("handshakeSuccess>>>");*/
        String newConnectTopic = "dwd_newconnect_log";
        String handshakeSuccessTopic = "dwd_handshake_success_log";
        String userLoginTopic = "dwd_user_login_log";
        String clientConnectTopic = "dwd_client_connect_log";
        String clientClosedTopic = "dwd_client_closed_log";
        String clientLoginTopic = "dwd_client_login_log";
        String clientConfigCenterTopic = "dwd_client_config_center_log";
        String clientSendTopic = "dwd_client_send_log";
        String linkTechTopic = "dwd_link_tech_log";
        String settingTopic = "dwd_setting_log";
        String linkProTopic = "dwd_remote_link_process_log";
        //String clickTopic = "dwd_click_log";
        String clickLoginTopic = "dwd_click_login_log";
        String newUserTopic = "new_user_log";

        backendNewConnectStream.addSink(KafkaUtil.getKafkaSink(newConnectTopic));
        backendHandshakeSuccessStream.addSink(KafkaUtil.getKafkaSink(handshakeSuccessTopic));
        userLoginStream.addSink(KafkaUtil.getKafkaSink(userLoginTopic));
        clientConnectStream.addSink(KafkaUtil.getKafkaSink(clientConnectTopic));
        clientClosedStream.addSink(KafkaUtil.getKafkaSink(clientClosedTopic));
        clientLoginStream.addSink(KafkaUtil.getKafkaSink(clientLoginTopic));
        clientConfigCenterStream.addSink(KafkaUtil.getKafkaSink(clientConfigCenterTopic));
        clientSendStream.addSink(KafkaUtil.getKafkaSink(clientSendTopic));
        linkTechStream.addSink(KafkaUtil.getKafkaSink(linkTechTopic));
        settingStream.addSink(KafkaUtil.getKafkaSink(settingTopic));
        linkProStream.addSink(KafkaUtil.getKafkaSink(linkProTopic));
        clickLoginStream.addSink(KafkaUtil.getKafkaSink(clickLoginTopic));
        newUserStream.addSink(KafkaUtil.getKafkaSink(newUserTopic));

        env.execute("AllLogApp");
    }
}