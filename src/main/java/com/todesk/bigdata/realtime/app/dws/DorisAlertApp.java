package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.DorisAlertBean;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.DorisUtil;
import com.todesk.bigdata.realtime.utils.EnvConfig;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Author : Liuchao
 * Date : 2023/8/17 17:35
 * Desc :
 */
public class DorisAlertApp {
    private static final HashSet<String> alertFlag = new HashSet<String>(Arrays.asList("b_login","c_login","b_closed","c_closed","b_newconnect_fail","c_newconnect_fail","b_logout","c_logout","b_connect","c_connect","b_newconnect","c_newconnect","http","password_limit","del_fserver","sendsms","sendemail","DuplexMessage","runtime"));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);

        String loginTopic = "doris_dwd_login_log";
        String clientTopic = "doris_dwd_client_con_log";
        String backendTopic = "doris_dwd_backend_log";
        String newFailTopic = "doris_dwd_risk_management_log";
        String groupId = "alert";
        //String groupId = "alert_test";

        //FlinkKafkaConsumer<String> loginSource1 = (FlinkKafkaConsumer<String>) KafkaUtil.getKafkaSource(loginTopic, groupId).setStartFromLatest();
        FlinkKafkaConsumer<String> loginSource = KafkaUtil.getKafkaSource(loginTopic, groupId);
        FlinkKafkaConsumer<String> clientSource = KafkaUtil.getKafkaSource(clientTopic, groupId);
        FlinkKafkaConsumer<String> backendSource = KafkaUtil.getKafkaSource(backendTopic, groupId);
        FlinkKafkaConsumer<String> newFailSource = KafkaUtil.getKafkaSource(newFailTopic, groupId);

        DataStreamSource<String> loginDS = env.addSource(loginSource).setParallelism(3);
        DataStreamSource<String> clientDS = env.addSource(clientSource).setParallelism(3);
        DataStreamSource<String> backendDS = env.addSource(backendSource).setParallelism(3);
        DataStreamSource<String> newFailDS = env.addSource(newFailSource).setParallelism(3);


        SingleOutputStreamOperator<DorisAlertBean> runtimeDS = clientDS
                .union(loginDS.filter(data -> "user_login".equals(JSON.parseObject(data).getString("logtype")) && "0".equals(JSON.parseObject(data).getString("enterprise"))))
                .filter(data -> {
                    String runtime = JSON.parseObject(data).getString("runtime");
                    return runtime != null
                            && Pattern.compile("-?\\d+(\\.\\d+)?").matcher(runtime).matches()
                            && Double.parseDouble(runtime) > 2000;
                })
                .map(data -> new DorisAlertBean("", "", "", "runtime", 1L, JSON.parseObject(data).getLong("logtime")));


        SingleOutputStreamOperator<DorisAlertBean> loginBeanDS = loginDS
                .filter(data -> {
                    JSONObject jsonData = JSON.parseObject(data);
                    String logtype = jsonData.getString("logtype");
                    String method = jsonData.getString("method");
                    String url = jsonData.getString("url");

                    return "user_login".equals(logtype)
                            || "user_logout".equals(logtype)
                            || "sendemail".equals(logtype)
                            || "DuplexMessage".equals(logtype)
                            || ("http".equals(logtype) && "DELETE".equals(method) && "/message-center/free-todesk/machine/".equals(url))
                            || "sendsms".equals(logtype);
                })
                .map(new MapFunction<String, DorisAlertBean>() {
                    @Override
                    public DorisAlertBean map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String resultmsg = jsonObject.getString("resultmsg");
                        String enterprise = jsonObject.getString("enterprise");
                        String logtype = jsonObject.getString("logtype");
                        Long logtime = jsonObject.getLong("logtime");

                        switch (logtype){
                            case "sendsms":
                            case "sendemail":
                            case "DuplexMessage":
                            case "http":
                                return new DorisAlertBean("","", "", logtype, 1L, logtime);
                            case "user_login":
                                if ("0".equals(enterprise)) {
                                    if ("password limit".equals(resultmsg)) {
                                        return new DorisAlertBean("","", "", "password_limit", 1L, logtime);
                                    } else {
                                        return new DorisAlertBean("","", "", "c_login", 1L, logtime);
                                    }
                                } else if ("1".equals(enterprise)) {
                                    return new DorisAlertBean("","", "", "b_login", 1L, logtime);
                                }
                            case "user_logout":
                                if ("1".equals(enterprise)) {
                                    return new DorisAlertBean("","", "", "b_logout", 1L, logtime);
                                } else if ("0".equals(enterprise)) {
                                    return new DorisAlertBean("","", "", "c_logout", 1L, logtime);
                                }
                            default:
                                break;
                        }
                        return null;
                    }
                }).setParallelism(3);

        SingleOutputStreamOperator<DorisAlertBean> clientBeanDS = clientDS.map(new MapFunction<String, DorisAlertBean>() {
            @Override
            public DorisAlertBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String enterprise = jsonObject.getString("enterprise");
                String logtype = jsonObject.getString("logtype");
                Long logtime = jsonObject.getLong("logtime");
                if ("client_connect".equals(logtype)) {
                    if ("0".equals(enterprise)) {
                        return new DorisAlertBean("","", "", "c_connect", 1L, logtime);
                    } else if ("1".equals(enterprise)) {
                        return new DorisAlertBean("","", "", "b_connect", 1L, logtime);
                    }
                } else if ("client_closed".equals(logtype)) {
                    if ("0".equals(enterprise)) {
                        return new DorisAlertBean("","", "", "c_closed", 1L, logtime);
                    } else if ("1".equals(enterprise)) {
                        return new DorisAlertBean("","", "", "b_closed", 1L, logtime);
                    }
                }
                return null;
            }
        }).setParallelism(3);

        SingleOutputStreamOperator<DorisAlertBean> backendBeanDS = backendDS
                .filter(data->{
                    JSONObject jsonData = JSON.parseObject(data);
                    String logtype = jsonData.getString("logtype");
                    String type = jsonData.getString("type");

                    return "newconnect".equals(logtype)
                            || ("del_fserver".equals(logtype) && "2".equals(type));
                })
                .map(new MapFunction<String, DorisAlertBean>() {
                    @Override
                    public DorisAlertBean map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String logtype = jsonObject.getString("logtype");
                        String enterprise = jsonObject.getString("enterprise");
                        String errno = jsonObject.getString("errno");
                        Long logtime = jsonObject.getLong("logtime");
                        switch (logtype){
                            case "newconnect":
                                if ("0".equals(enterprise)) {
                                    return new DorisAlertBean("","", "", "c_newconnect", 1L, logtime);
                                } else if ("1".equals(enterprise)) {
                                    if (!"0".equals(errno)) {
                                        return new DorisAlertBean("","", "", "b_newconnect", 1L, logtime);
                                    } else {
                                        return new DorisAlertBean("","", "", "b_newconnect_fail", 1L, logtime);
                                    }
                                }
                            case "del_fserver":
                                return new DorisAlertBean("","", "", logtype, 1L, logtime);
                            default:
                                break;
                        }
                        return null;
                    }
                }).setParallelism(3);


        SingleOutputStreamOperator<DorisAlertBean> newFailBeanDS = newFailDS
                .filter(data -> JSON.parseObject(data).getString("logtype").equals("newconnect_fail"))
                .map(new MapFunction<String, DorisAlertBean>() {
                    @Override
                    public DorisAlertBean map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String enterprise = jsonObject.getString("enterprise");
                        Long logtime = jsonObject.getLong("logtime");
                        if ("0".equals(enterprise)) {
                            return new DorisAlertBean("","", "", "c_newconnect_fail", 1L, logtime);
                        }
                        return null;
                    }
                }).setParallelism(3);

        DataStream<DorisAlertBean> alertDS = loginBeanDS.filter(Objects::nonNull)
                .union(clientBeanDS.filter(Objects::nonNull))
                .union(backendBeanDS.filter(Objects::nonNull))
                .union(newFailBeanDS.filter(Objects::nonNull))
                .union(runtimeDS.filter(Objects::nonNull));

        SingleOutputStreamOperator<DorisAlertBean> alertDSWithWatermark = alertDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DorisAlertBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DorisAlertBean>() {
                            @Override
                            public long extractTimestamp(DorisAlertBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );


        KeyedStream<DorisAlertBean, String> dorisAlertBeanStringKeyedStream = alertDSWithWatermark.keyBy(DorisAlertBean::getFlag);


        SingleOutputStreamOperator<DorisAlertBean> reduceDS = dorisAlertBeanStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60))).reduce(
                new ReduceFunction<DorisAlertBean>() {
                    @Override
                    public DorisAlertBean reduce(DorisAlertBean value1, DorisAlertBean value2) throws Exception {
                        value1.setCnt(value1.getCnt() + value2.getCnt());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DorisAlertBean, DorisAlertBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DorisAlertBean> elements, Collector<DorisAlertBean> out) throws Exception {

                        String dt = DateFormatUtil.toDate(context.window().getStart());
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                        for (String flag: alertFlag
                        ) {
                            out.collect(new DorisAlertBean(dt,stt,edt,flag,0L,System.currentTimeMillis()));
                        }

                        elements.iterator().next().setDt(dt);
                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setTs(System.currentTimeMillis());

                        out.collect(elements.iterator().next());
                    }
                }
        ).setParallelism(3);

        SingleOutputStreamOperator<String> resultDS = reduceDS.keyBy(data -> data.getStt() + data.getFlag())
                .process(
                        new KeyedProcessFunction<String, DorisAlertBean, String>() {
                            private ValueState<DorisAlertBean> dataDefaultValue;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                dataDefaultValue = getRuntimeContext().getState(new ValueStateDescriptor<DorisAlertBean>("alert_default", DorisAlertBean.class));
                            }

                            @Override
                            public void processElement(DorisAlertBean value, Context ctx, Collector<String> out) throws Exception {
                                DorisAlertBean defaultData = dataDefaultValue.value();

                                if (defaultData == null) {
                                    dataDefaultValue.update(value);
                                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
                                } else {
                                    Long stateCnt = defaultData.getCnt();
                                    Long curCnt = value.getCnt();

                                    if (curCnt > stateCnt) {
                                        dataDefaultValue.update(value);
                                    }
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                String defaultData = JSONObject.toJSONString(dataDefaultValue.value());
                                out.collect(defaultData);
                                dataDefaultValue.clear();
                            }
                        }
                ).setParallelism(3);


        //resultDS.print();
        resultDS.addSink(DorisUtil.getDorisSink("todesk.dws_all_alert",2));

        env.execute("DorisAlertApp");

    }
}
