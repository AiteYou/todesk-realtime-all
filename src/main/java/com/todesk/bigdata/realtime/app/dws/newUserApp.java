package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.ClientSettingBean;
import com.todesk.bigdata.realtime.bean.newUserBean;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/11/23 18:51
 * Desc :
 */
public class newUserApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Configuration conf = new Configuration();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.SECONDS),Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");

        String newUserTopic = "new_user_log";
        String groupId = "dws_client_total_group";
        DataStreamSource<String> clientSendDS = env.addSource(KafkaUtil.getKafkaSource(newUserTopic, groupId)).setParallelism(3);
        SingleOutputStreamOperator<JSONObject> jsonObjNewUserDS = clientSendDS.map(JSON::parseObject).setParallelism(3);

        SingleOutputStreamOperator<newUserBean> newUserBeanStream = jsonObjNewUserDS.map(new MapFunction<JSONObject, newUserBean>() {
            @Override
            public newUserBean map(JSONObject jsonObj) throws Exception {
                // 获取时间戳
                String platform = jsonObj.getString("platform");
                Long ts = jsonObj.getLong("logtime");
                Long userCount = 1L;
                newUserBean backendNewUserBean = new newUserBean(
                        "",
                        "",
                        platform,
                        userCount,
                        ts
                );

                return backendNewUserBean;
            }
        });

        SingleOutputStreamOperator<newUserBean> jsonObjWithWatermark = newUserBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<newUserBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<newUserBean>() {
                            @Override
                            public long extractTimestamp(newUserBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        KeyedStream<newUserBean, String> newUserBeanStringKeyedStream = jsonObjWithWatermark.keyBy(newUserBean::getPlatform);

        SingleOutputStreamOperator<newUserBean> reduceDS = newUserBeanStringKeyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60*5))).reduce(
                new ReduceFunction<newUserBean>() {
                    @Override
                    public newUserBean reduce(newUserBean value1, newUserBean value2) throws Exception {
                        value1.setCount(value1.getCount() + value2.getCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<newUserBean, newUserBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<newUserBean> elements, Collector<newUserBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        out.collect(elements.iterator().next());
                    }
                }
        ).setParallelism(3);

        SinkFunction<newUserBean> jdbcSink = ClickHouseUtil.<newUserBean>getJdbcSink(
                "insert into dws_new_user_window values(?,?,?,?,?)");

        reduceDS.addSink(jdbcSink).setParallelism(1);

        env.execute("newUserAPP");
    }
}
