package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.BackendUserLoginBean;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/6/5 18:08
 * Desc :
 */
public class BackendUserLoginApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         /*Configuration conf = new Configuration();
        conf.getString(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);*/

        //设置并行度，和kafka分区数保持一致
        env.setParallelism(3);

        //TODO 2.设置检查点

        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        //Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        //cancel job时，Checkpoint是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //job失败，重启策略 5分钟内若失败了3次则认为该job失败，重试间隔为10s
        //设置失败重启作业3次 触发机制 失败3秒 作业延迟 2秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.SECONDS),Time.of(2,TimeUnit.SECONDS)));

        //设置状态后端1.13版本更新了此API
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        //System.setProperty("HADOOP_USER_NAME", "bigdata");

        String userLoginTopic = "dwd_user_login_log";
        String groupId = "dws_backend_total_group";
        DataStreamSource<String> backendUserLgoinSource = env.addSource(KafkaUtil.getKafkaSource(userLoginTopic, groupId));

        SingleOutputStreamOperator<JSONObject> backendUserLgoinStream = backendUserLgoinSource.map(JSON::parseObject);

        SingleOutputStreamOperator<BackendUserLoginBean> backendUserLoginBeanStream = backendUserLgoinStream.map(new MapFunction<JSONObject, BackendUserLoginBean>() {
            @Override
            public BackendUserLoginBean map(JSONObject jsonObj) throws Exception {
                // 获取时间戳
                Long ts = jsonObj.getLong("logtime");
                Long userLoginCount = 1L;
                BackendUserLoginBean backendUserLoginBean = new BackendUserLoginBean(
                        "",
                        "",
                        userLoginCount,
                        ts
                );

                return backendUserLoginBean;
            }
        });

        SingleOutputStreamOperator<BackendUserLoginBean> withWatermarkStream = backendUserLoginBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<BackendUserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<BackendUserLoginBean>() {
                                    @Override
                                    public long extractTimestamp(BackendUserLoginBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<BackendUserLoginBean> reduceStream = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
                .reduce(
                        new ReduceFunction<BackendUserLoginBean>() {
                            @Override
                            public BackendUserLoginBean reduce(BackendUserLoginBean value1, BackendUserLoginBean value2) throws Exception {
                                value1.setUserLoginCount(value1.getUserLoginCount() + value2.getUserLoginCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<BackendUserLoginBean, BackendUserLoginBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<BackendUserLoginBean> elements, Collector<BackendUserLoginBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                                elements.iterator().next().setStt(stt);
                                elements.iterator().next().setEdt(edt);
                                elements.iterator().next().setTs(System.currentTimeMillis());
                                out.collect(elements.iterator().next());
                            }
                        }
                );
        reduceStream.setParallelism(1);

        SinkFunction<BackendUserLoginBean> jdbcSink = ClickHouseUtil.<BackendUserLoginBean>getJdbcSink("insert into dws_backend_user_login_window values(?,?,?,?)");

        reduceStream.addSink(jdbcSink).setParallelism(1);

        env.execute("BackendUserLoginApp");

    }
}
