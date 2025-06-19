package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.AndroidLogicBean;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/7/7 19:46
 * Desc :
 */
public class AndroidLogicApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.SECONDS),Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");

        String logicTopic = "dwd_link_tech_log";
        String groupId = "dws_android_logic_group";
        //String groupId = "tes1";

        DataStreamSource<String> logicDS = env.addSource(KafkaUtil.getKafkaSource(logicTopic, groupId)).setParallelism(3);

        SingleOutputStreamOperator<JSONObject> logicJsonDS = logicDS.map(JSON::parseObject).setParallelism(3);

        SingleOutputStreamOperator<AndroidLogicBean> androidLogicBeanSingleOutputStreamOperator = logicJsonDS.flatMap(
                new FlatMapFunction<JSONObject, AndroidLogicBean>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<AndroidLogicBean> out) throws Exception {
                        String action = value.getString("action");
                        String platform = value.getString("platform");
                        String version = value.getString("version");
                        JSONObject turndata = null;
                        try {
                            turndata = value.getJSONObject("turndata");
                        } catch (Exception e) {
                            System.out.println(value);
                            e.printStackTrace();
                            return;
                        }
                        String resultCode = turndata.getString("result_code");
                        Long ts = value.getLong("logtime");
                        if (version.compareTo("4.4.3") >= 0) {
                            if ("20".equals(action)) {
                                out.collect(
                                        new AndroidLogicBean("", "", platform, version, "center_connect", 0L, 1L, 0.00,ts)
                                );
                            } else if ("21".equals(action) && "1".equals(resultCode)) {
                                out.collect(
                                        new AndroidLogicBean("", "", platform, version, "center_connect", 1L, 0L, 0.00,ts)
                                );
                           // } else if ("29".equals(action) && "iOS".equals(platform)) {
                            } else if ("29".equals(action) ) {
                                out.collect(
                                        new AndroidLogicBean("", "", platform, version, "center_login", 0L, 1L, 0.00,ts)
                                );
                            //} else if ("15".equals(action) && "iOS".equals(platform)) {
                            } else if ("15".equals(action) ) {
                                out.collect(
                                        new AndroidLogicBean("", "", platform, version, "center_login", 1L, 0L, 0.00,ts)
                                );
                            }
                        }
                    }
                }
        ).setParallelism(3);

        //androidLogicBeanSingleOutputStreamOperator.print();


        SingleOutputStreamOperator<AndroidLogicBean> androidLogicBeanWithWatermark = androidLogicBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AndroidLogicBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<AndroidLogicBean>() {
                            @Override
                            public long extractTimestamp(AndroidLogicBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        KeyedStream<AndroidLogicBean, String> androidLogicBeanKeyedDS = androidLogicBeanWithWatermark.keyBy(data -> (data.getPlatform() + data.getVersion() + data.getType()));

        SingleOutputStreamOperator<AndroidLogicBean> reduceDS = androidLogicBeanKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60))).reduce(
                new ReduceFunction<AndroidLogicBean>() {
                    @Override
                    public AndroidLogicBean reduce(AndroidLogicBean value1, AndroidLogicBean value2) throws Exception {
                        value1.setSuccessCount(value1.getSuccessCount() + value2.getSuccessCount());
                        value1.setTotalCount(value1.getTotalCount() + value2.getTotalCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<AndroidLogicBean, AndroidLogicBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<AndroidLogicBean> elements, Collector<AndroidLogicBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        DecimalFormat df = new DecimalFormat("######0.00");
                        double d = 0.0;

                        if (elements.iterator().next().getTotalCount() != 0) {
                            d = (double) (elements.iterator().next().getSuccessCount()) / elements.iterator().next().getTotalCount() * 100;
                        } else d = 0.0;
                        String successRateStr = df.format(d);
                        Double successRateDou = Double.valueOf(successRateStr.toString());

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setSuccessRate(successRateDou);
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        out.collect(elements.iterator().next());
                    }
                }
        ).setParallelism(3);

        //reduceDS.print();

        SinkFunction<AndroidLogicBean> jdbcSink = ClickHouseUtil.<AndroidLogicBean>getJdbcSink(
                "insert into dws_android_logic_window values(?,?,?,?,?,?,?,?,?)");


        reduceDS.addSink(jdbcSink).setParallelism(1);


        env.execute("AndroidLogicApp");
    }
}
