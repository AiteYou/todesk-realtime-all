package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.ClientConnectLineBean;
import com.todesk.bigdata.realtime.function.ClientFlatMapFunction;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/6/5 19:26
 * Desc :
 */
public class BackendClientConnectApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度，和kafka分区数保持一致

        //TODO 2.设置检查点

        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        //Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        //cancel job时，Checkpoint是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //job失败，重启策略 5分钟内若失败了3次则认为该job失败，重试间隔为10s
        //设置失败重启作业3次 触发机制 失败3秒 作业延迟 2秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(3, TimeUnit.SECONDS), org.apache.flink.api.common.time.Time.of(2,TimeUnit.SECONDS)));

        //设置状态后端1.13版本更新了此API
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        //System.setProperty("HADOOP_USER_NAME", "bigdata");

        String clientConnectTopic = "dwd_client_connect_log";
        //String groupId = "test";
        String groupId = "dws_backend_total_group";
        DataStreamSource<String> backendClientConnectSource = env.addSource(KafkaUtil.getKafkaSource(clientConnectTopic, groupId)).setParallelism(3);
        SingleOutputStreamOperator<JSONObject> backendClientConnectStream = backendClientConnectSource.map(JSON::parseObject).setParallelism(3);

        SingleOutputStreamOperator<ClientConnectLineBean> ETLbackendClientConnectStream = backendClientConnectStream.flatMap(new ClientFlatMapFunction());

        SingleOutputStreamOperator<ClientConnectLineBean> withWatermarkStream = ETLbackendClientConnectStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClientConnectLineBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ClientConnectLineBean>() {
                                    @Override
                                    public long extractTimestamp(ClientConnectLineBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        ).setParallelism(3);

        KeyedStream<ClientConnectLineBean, String> keyedStream = withWatermarkStream.keyBy(data -> (data.getLine() + data.getProvince()));

        SingleOutputStreamOperator<ClientConnectLineBean> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60))).reduce(
                new ReduceFunction<ClientConnectLineBean>() {
                    @Override
                    public ClientConnectLineBean reduce(ClientConnectLineBean value1, ClientConnectLineBean value2) throws Exception {
                        value1.setClientConnectCount(value1.getClientConnectCount() + value2.getClientConnectCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<ClientConnectLineBean, ClientConnectLineBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ClientConnectLineBean> elements, Collector<ClientConnectLineBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        out.collect(elements.iterator().next());
                    }
                }
        ).setParallelism(3);

        SinkFunction<ClientConnectLineBean> jdbcSink = ClickHouseUtil.<ClientConnectLineBean>getJdbcSink("insert into dws_line_client_connect_window values(?,?,?,?,?,?)");

        reduceDS.addSink(jdbcSink).setParallelism(1);
        //reduceDS.print();

        env.execute("BackendClientConnectApp");
    }
}
