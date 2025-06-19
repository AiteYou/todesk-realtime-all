package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.ClientLoginBean;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/6/16 18:26
 * Desc :
 */
public class ClientLoginApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Configuration conf = new Configuration();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(3, TimeUnit.SECONDS), org.apache.flink.api.common.time.Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");

        String clientLoginTopic = "dwd_client_login_log";
        String groupId = "dws_client_total_group";
        DataStreamSource<String> clientLoginSource = env.addSource(KafkaUtil.getKafkaSource(clientLoginTopic, groupId)).setParallelism(3);

        SingleOutputStreamOperator<JSONObject> clientLoginStream = clientLoginSource.map(JSON::parseObject).setParallelism(3);

        /*SingleOutputStreamOperator<ClientLoginBean> clientLoginBeanMap = clientLoginStream.map(
                new MapFunction<JSONObject, ClientLoginBean>() {
                    @Override
                    public ClientLoginBean map(JSONObject value) throws Exception {
                        String logtype = value.getString("logtype");
                        Long ts = value.getLong("logtime");
                        String ckUrl = value.getString("ck_url");

                        if ("phoneLogin".equals(logtype)) {
                            return new ClientLoginBean(
                                    "", "", "phoneLogin", 0L, 1L, ts
                            );
                        } else ("errorlog".equals(logtype) && "/api/phone/verifyPhoneCode".equals(ckUrl)) {
                            return new ClientLoginBean(
                                    "", "", "phoneLogin", 1L, 1L, ts
                            );
                        }
                    }
                }
        );*/

        SingleOutputStreamOperator<ClientLoginBean> clientLoginBeanSingleOutputStreamOperator = clientLoginStream.flatMap(
                new FlatMapFunction<JSONObject, ClientLoginBean>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<ClientLoginBean> out) throws Exception {
                        String logtype = value.getString("logtype");
                        Long ts = value.getLong("logtime");
                        String ckUrl = value.getString("ck_url");

                        if ("autoLogin".equals(logtype)) {
                            out.collect(
                                    new ClientLoginBean(
                                            "", "", "autoLogin", 0L, 1L, ts
                                    )

                            );
                        } else if ("phoneLogin".equals(logtype)) {
                            out.collect(
                                    new ClientLoginBean(
                                            "", "", "phoneLogin", 0L, 1L, ts
                                    )
                            );
                        } else if ("errorlog".equals(logtype) && "/api/qrcode/autoLogin".equals(ckUrl)){
                            out.collect(
                                    new ClientLoginBean(
                                            "", "", "autoLogin", 1L, 1L, ts
                                    )
                            );
                        } else if ("errorlog".equals(logtype) && "/api/phone/verifyPhoneCode".equals(ckUrl)) {
                            out.collect(
                                    new ClientLoginBean(
                                    "", "", "phoneLogin", 1L, 1L, ts
                                    )
                            );
                        }
                    }
                }
        ).setParallelism(3);

        //clientLoginBeanSingleOutputStreamOperator.print();
        //client
        //clientLoginBeanSingleOutputStreamOperator.keyBy();


        SingleOutputStreamOperator<ClientLoginBean> jsonObjWithWatermark = clientLoginBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClientLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClientLoginBean>() {
                            @Override
                            public long extractTimestamp(ClientLoginBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        KeyedStream<ClientLoginBean, String> clientLoginBeanStringKeyedStream = jsonObjWithWatermark.keyBy(ClientLoginBean::getLoginType);

        SingleOutputStreamOperator<ClientLoginBean> reduceDS = clientLoginBeanStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60))).reduce(
                new ReduceFunction<ClientLoginBean>() {
                    @Override
                    public ClientLoginBean reduce(ClientLoginBean value1, ClientLoginBean value2) throws Exception {
                        value1.setLoginTotalCount(value1.getLoginTotalCount() + value2.getLoginTotalCount());
                        value1.setLoginFailedCount(value1.getLoginFailedCount() + value2.getLoginFailedCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<ClientLoginBean, ClientLoginBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ClientLoginBean> elements, Collector<ClientLoginBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        out.collect(elements.iterator().next());
                    }
                }
        ).setParallelism(3);

        SinkFunction<ClientLoginBean> jdbcSink = ClickHouseUtil.<ClientLoginBean>getJdbcSink(
                "insert into dws_client_login_window values(?,?,?,?,?,?)");

        reduceDS.addSink(jdbcSink).setParallelism(1);

        env.execute("ClientLoginApp");
    }
}