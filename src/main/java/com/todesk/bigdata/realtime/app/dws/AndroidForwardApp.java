package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.AndroidForwardBean;
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
 * Date : 2022/8/9 19:52
 * Desc :
 */
public class AndroidForwardApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.SECONDS),Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");

        String logicTopic = "dwd_remote_link_process_log";
        String groupId = "dws_android_forward_group";

        DataStreamSource<String> forwardDS = env.addSource(KafkaUtil.getKafkaSource(logicTopic, groupId)).setParallelism(3);

        SingleOutputStreamOperator<JSONObject> forwardJsonDS = forwardDS.map(JSON::parseObject).setParallelism(3);

        SingleOutputStreamOperator<AndroidForwardBean> androidForwardBeanSingleOutputStreamOperator = forwardJsonDS.flatMap(
                new FlatMapFunction<JSONObject, AndroidForwardBean>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<AndroidForwardBean> out) throws Exception {
                        String action = value.getString("action");
                        String platform = value.getString("platform");
                        String version = value.getString("version");
                        JSONObject turndata = value.getJSONObject("turndata");
                        Long ts = value.getLong("logtime");
                        if (version.compareTo("4.4.6") >= 0) {

                            if ("state_control_start".equals(action)) {
                                out.collect(
                                        new AndroidForwardBean("", "", platform, version, "tcp", 0L, 1L, 0.00, ts)
                                );
                                out.collect(
                                        new AndroidForwardBean("", "", platform, version, "password", 0L, 1L, 0.00, ts)
                                );
                                out.collect(
                                        new AndroidForwardBean("", "", platform, version, "certify", 0L, 1L, 0.00, ts)
                                );
                            }else if("state_control".equals(action)){
                                String stateForwardConn = turndata.getString("STATE_FORWARD_CONN");
                                String statePwdVerify = turndata.getString("STATE_PWD_VERIFY");
                                String stateCerVerify = turndata.getString("STATE_CER_VERIFY");
                                if ("1".equals(stateForwardConn)){
                                    out.collect(
                                            new AndroidForwardBean("", "", platform, version, "tcp", 1L, 0L, 0.00, ts)
                                    );
                                }
                                if ("1".equals(statePwdVerify)){
                                    out.collect(
                                            new AndroidForwardBean("", "", platform, version, "password", 1L, 0L, 0.00, ts)
                                    );
                                }
                                if ("1".equals(stateCerVerify)){
                                    out.collect(
                                            new AndroidForwardBean("", "", platform, version, "certify", 1L, 0L, 0.00, ts)
                                    );
                                }
                            }
                        }
                    }
                }
        ).setParallelism(3);

        SingleOutputStreamOperator<AndroidForwardBean> androidLogicBeanWithWatermark = androidForwardBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AndroidForwardBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<AndroidForwardBean>() {
                            @Override
                            public long extractTimestamp(AndroidForwardBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        KeyedStream<AndroidForwardBean, String> androidForwardBeanKeyedDS = androidLogicBeanWithWatermark.keyBy(data -> (data.getPlatform() + data.getVersion() + data.getType()));

        SingleOutputStreamOperator<AndroidForwardBean> reduceDS = androidForwardBeanKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60))).reduce(
                new ReduceFunction<AndroidForwardBean>() {
                    @Override
                    public AndroidForwardBean reduce(AndroidForwardBean value1, AndroidForwardBean value2) throws Exception {
                        value1.setSuccessCount(value1.getSuccessCount() + value2.getSuccessCount());
                        value1.setTotalCount(value1.getTotalCount() + value2.getTotalCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<AndroidForwardBean, AndroidForwardBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<AndroidForwardBean> elements, Collector<AndroidForwardBean> out) throws Exception {
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

        //androidForwardBeanSingleOutputStreamOperator.print();

        SinkFunction<AndroidForwardBean> jdbcSink = ClickHouseUtil.<AndroidForwardBean>getJdbcSink(
                "insert into dws_android_forward_window values(?,?,?,?,?,?,?,?,?)");


        reduceDS.addSink(jdbcSink).setParallelism(1);


        env.execute("AndroidForwardApp");
    }
}
