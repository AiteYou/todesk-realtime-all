package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.BackendClientClosedBean;
import com.todesk.bigdata.realtime.bean.ClientConnectLineBean;
import com.todesk.bigdata.realtime.function.ClientFlatMapFunction;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
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
 * Date : 2022/6/28 12:27
 * Desc :
 */
public class BackendClientClosedApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(3, TimeUnit.SECONDS), org.apache.flink.api.common.time.Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");

        String clientClosedTopic = "dwd_client_closed_log";
        String groupId = "dws_backend_client_closed_group";
        //String groupId = "test";


        DataStreamSource<String> backendClientClosedSource = env.addSource(KafkaUtil.getKafkaSource(clientClosedTopic, groupId)).setParallelism(3);
        SingleOutputStreamOperator<JSONObject> backendClientClosedStream = backendClientClosedSource.map(JSON::parseObject).setParallelism(3);

        SingleOutputStreamOperator<ClientConnectLineBean> ETLbackendClientConnectStream = backendClientClosedStream.flatMap(new ClientFlatMapFunction());

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


        /*SingleOutputStreamOperator<BackendClientClosedBean> backendClientClosedBeanStream = backendClientClosedStream.map(
                new MapFunction<JSONObject, BackendClientClosedBean>() {
                    @Override
                    public BackendClientClosedBean map(JSONObject jsonObj) throws Exception {
                        Long ts = jsonObj.getLong("logtime");

                        BackendClientClosedBean backendClientClosedBeanBean = new BackendClientClosedBean(
                                "",
                                "",
                                1L,
                                ts
                        );
                        return backendClientClosedBeanBean;
                    }
                }
        ).setParallelism(3);*/

        /*SingleOutputStreamOperator<BackendClientClosedBean> withWatermarkStream = backendClientClosedBeanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<BackendClientClosedBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<BackendClientClosedBean>() {
                                    @Override
                                    public long extractTimestamp(BackendClientClosedBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<BackendClientClosedBean> reduceStream = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
                .reduce(
                        new ReduceFunction<BackendClientClosedBean>() {
                            @Override
                            public BackendClientClosedBean reduce(BackendClientClosedBean value1, BackendClientClosedBean value2) throws Exception {
                                value1.setClientClosedCount(value1.getClientClosedCount() + value2.getClientClosedCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<BackendClientClosedBean, BackendClientClosedBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<BackendClientClosedBean> elements, Collector<BackendClientClosedBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                                elements.iterator().next().setStt(stt);
                                elements.iterator().next().setEdt(edt);
                                elements.iterator().next().setTs(System.currentTimeMillis());
                                out.collect(elements.iterator().next());
                            }
                        }
                ).setParallelism(3);

        SinkFunction<BackendClientClosedBean> jdbcSink = ClickHouseUtil.<BackendClientClosedBean>getJdbcSink("insert into dws_backend_client_closed_window values(?,?,?,?)");

        reduceStream.addSink(jdbcSink).setParallelism(1);*/

        SinkFunction<ClientConnectLineBean> jdbcSink = ClickHouseUtil.<ClientConnectLineBean>getJdbcSink("insert into dws_line_client_closed_window values(?,?,?,?,?,?)");

        reduceDS.addSink(jdbcSink).setParallelism(1);

        env.execute("BackendClientClosedApp");
    }
}
