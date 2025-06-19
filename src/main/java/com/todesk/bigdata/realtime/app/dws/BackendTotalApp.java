package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.BackendTotalBean;
import com.todesk.bigdata.realtime.common.TodeskConfig;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/5/9 19:31
 * Desc : 连通率汇总(没有区分维度)
 */
public class BackendTotalApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
       /* Configuration conf = new Configuration();
        conf.getString(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);*/

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        // TODO 3.封装为流
        String newConnectTopic = "dwd_newconnect_log";
        String groupId = "dws_backend_total_group";
        DataStreamSource<String> backendNewConnectSource = env.addSource(KafkaUtil.getKafkaSource(newConnectTopic, groupId));


        // TODO 4. 转换页面流数据结构
        SingleOutputStreamOperator<JSONObject> backendNewConnectStream = backendNewConnectSource.map(JSON::parseObject);

        SingleOutputStreamOperator<BackendTotalBean> backendNewConnectBeanStream  = backendNewConnectStream.map(new MapFunction<JSONObject, BackendTotalBean>() {
            @Override
            public BackendTotalBean map(JSONObject jsonObj) throws Exception {
                // 获取时间戳
                Long ts = jsonObj.getLong("logtime");

                // 定义变量接受其它度量值
                Long newConnectCount = 1L;
                Long handshakeSuccessCount = 0L;
                Double handshakeSuccessRate = 0.00;

                BackendTotalBean backendTotalBean = new BackendTotalBean(
                        "",
                        "",
                        newConnectCount,
                        handshakeSuccessCount,
                        handshakeSuccessRate,
                        ts
                );
                return backendTotalBean;
            }
        });

        String handshakeSuccessTopic = "dwd_handshake_success_log";
        DataStreamSource<String> handshakeSuccessSource = env.addSource(KafkaUtil.getKafkaSource(handshakeSuccessTopic, groupId));
        SingleOutputStreamOperator<JSONObject> handshakeSuccessStream = handshakeSuccessSource.map(JSON::parseObject);
        SingleOutputStreamOperator<BackendTotalBean> handshakeSuccessBeanStream = handshakeSuccessStream.map(new MapFunction<JSONObject, BackendTotalBean>() {
            @Override
            public BackendTotalBean map(JSONObject jsonObj) throws Exception {
                // 获取时间戳
                Long ts = jsonObj.getLong("logtime");

                // 定义变量接受其它度量值
                Long newConnectCount = 0L;
                Long handshakeSuccessCount = 1L;
                Double handshakeSuccessRate = 0.00;


                BackendTotalBean backendTotalBean = new BackendTotalBean(
                        "",
                        "",
                        newConnectCount,
                        handshakeSuccessCount,
                        handshakeSuccessRate,
                        ts
                );
                return backendTotalBean;
            }
        });

        DataStream<BackendTotalBean> unionDS = backendNewConnectBeanStream.union(handshakeSuccessBeanStream);

        SingleOutputStreamOperator<BackendTotalBean> withWatermarkStream = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                    /*.<BackendTotalBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<BackendTotalBean>() {
                            @Override
                            public long extractTimestamp(BackendTotalBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }
                    )*/
                    .<BackendTotalBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner(
                            new SerializableTimestampAssigner<BackendTotalBean>() {
                                @Override
                                public long extractTimestamp(BackendTotalBean element, long recordTimestamp) {
                                return element.getTs();
                                }
                            }
                    )
        );

        SingleOutputStreamOperator<BackendTotalBean> reduceStream = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
                // 增量聚合
                .reduce(
                        new ReduceFunction<BackendTotalBean>() {
                            @Override
                            public BackendTotalBean reduce(BackendTotalBean value1, BackendTotalBean value2) throws Exception {
                                value1.setNewConnectCount(value1.getNewConnectCount() + value2.getNewConnectCount());
                                value1.setHandshakeSuccessCount(value1.getHandshakeSuccessCount() + value2.getHandshakeSuccessCount());
                                //value1.setHandshakeSuccessRate((double) (value1.getHandshakeSuccessCount() + value2.getHandshakeSuccessCount() / value1.getNewConnectCount() + value2.getNewConnectCount()));
                                return value1;
                            }
                        },
                        // 全量聚合，内存占用高，但可以获取窗口
                        new ProcessAllWindowFunction<BackendTotalBean, BackendTotalBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<BackendTotalBean> elements, Collector<BackendTotalBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                DecimalFormat    df   = new DecimalFormat("######0.00");

                                double d = 0.0;
                                if( elements.iterator().next().getNewConnectCount() != 0 ){
                                    d = (double) (elements.iterator().next().getHandshakeSuccessCount()) / elements.iterator().next().getNewConnectCount() * 100;
                                }   else d = 0.0;

                                String handrateStr = df.format(d);
                                Double handrateDou = Double.valueOf(handrateStr.toString());


                                //Double handrateDou = (double) (elements.iterator().next().getHandshakeSuccessCount()) / elements.iterator().next().getNewConnectCount() * 100;

                                elements.iterator().next().setStt(stt);
                                elements.iterator().next().setEdt(edt);
                                elements.iterator().next().setHandshakeSuccessRate(handrateDou);
                                elements.iterator().next().setTs(System.currentTimeMillis());
                                out.collect(elements.iterator().next());

                                /*for (BackendTotalBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }*/

                                /*out.collect(new BackendTotalBean(
                                        stt,
                                        edt,
                                        elements.iterator().next().getNewConnectCount(),
                                        elements.iterator().next().getHandshakeSuccessCount(),
                                        (double) (elements.iterator().next().getHandshakeSuccessCount() / elements.iterator().next().getNewConnectCount() *100),
                                        System.currentTimeMillis()
                                ));*/
                            }
                        });

        SinkFunction<BackendTotalBean> jdbcSink = ClickHouseUtil.<BackendTotalBean>getJdbcSink(
                "insert into dws_backend_total_window values(?,?,?,?,?,?)");

        /*reduceStream.addSink(
                JdbcSink.sink(
                        "insert into dws_backend_total_window values(?,?,?,?,?,?)",
                        (statement,tp ) -> {
                            statement.setString(1, tp.getStt());
                            statement.setString(2, tp.getEdt());
                            statement.setLong(3, tp.getNewConnectCount());
                            statement.setLong(4, tp.getHandshakeSuccessCount());
                            statement.setDouble(5, tp.getHandshakeSuccessRate());
                            statement.setLong(6, tp.getTs());

                        },
                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(0)
                                .withBatchSize(1)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName(TodeskConfig.CLICKHOUSE_DRIVER)
                                .withUrl(TodeskConfig.CLICKHOUSE_URL)
                                .withPassword("123456")
                                .build()
                )
        );*/

       reduceStream.addSink(jdbcSink).setParallelism(1);

        //reduceStream.print(">>>>");

        env.execute("BackendTotalApp");
    }
}
