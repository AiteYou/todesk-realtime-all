package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.ClientSendBean;
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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/6/23 19:38
 * Desc :
 */
public class ClientSendTotalApp {
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

        String clientSendTopic = "dwd_client_send_log";
        String groupId = "dws_client_total_group";
        DataStreamSource<String> clientSendDS = env.addSource(KafkaUtil.getKafkaSource(clientSendTopic, groupId)).setParallelism(3);
        SingleOutputStreamOperator<JSONObject> jsonObjClientSendDS = clientSendDS.map(JSON::parseObject).setParallelism(3);

        SingleOutputStreamOperator<ClientSendBean> clientSendBeanSingleOutputStreamOperator = jsonObjClientSendDS.flatMap(
                new FlatMapFunction<JSONObject, ClientSendBean>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<ClientSendBean> out) throws Exception {
                        String logtype = value.getString("logtype");
                        String errno = value.getString("errno");
                        Long ts = value.getLong("logtime");

                        if ("sendemail".equals(logtype) && !"0".equals(errno)) {
                            out.collect(
                                    new ClientSendBean(
                                            "", "", logtype, 1L, 0L, ts
                                    ));
                        } else if ("sendemail".equals(logtype) && "0".equals(errno)) {
                            out.collect(
                                    new ClientSendBean(
                                            "", "", logtype, 1L, 1L, ts
                                    )
                            );
                        } else if ("sendsms".equals(logtype) && !"0".equals(errno)) {
                            out.collect(
                                    new ClientSendBean(
                                            "", "", logtype, 1L, 0L, ts
                                    )
                            );
                        } else if ("sendsms".equals(logtype) && "0".equals(errno)) {
                            out.collect(
                                    new ClientSendBean(
                                            "", "", logtype, 1L, 1L, ts
                                    )
                            );
                        }
                    }
                }
        ).setParallelism(3);

        SingleOutputStreamOperator<ClientSendBean> jsonObjWithWatermark = clientSendBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClientSendBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClientSendBean>() {
                            @Override
                            public long extractTimestamp(ClientSendBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        KeyedStream<ClientSendBean, String> clientSendBeanStringKeyedStream = jsonObjWithWatermark.keyBy(ClientSendBean::getSendType);

        SingleOutputStreamOperator<ClientSendBean> reduceDS = clientSendBeanStringKeyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60))).reduce(
                new ReduceFunction<ClientSendBean>() {
                    @Override
                    public ClientSendBean reduce(ClientSendBean value1, ClientSendBean value2) throws Exception {
                        value1.setSendFailedCount(value1.getSendFailedCount() + value2.getSendFailedCount());
                        value1.setSendTotalCount(value1.getSendTotalCount() + value2.getSendTotalCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<ClientSendBean, ClientSendBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ClientSendBean> elements, Collector<ClientSendBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setSendFailedCount(elements.iterator().next().getSendTotalCount()-elements.iterator().next().getSendFailedCount());
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        out.collect(elements.iterator().next());
                    }
                }
        ).setParallelism(3);

        SinkFunction<ClientSendBean> jdbcSink = ClickHouseUtil.<ClientSendBean>getJdbcSink(
                "insert into dws_client_send_window values(?,?,?,?,?,?)");

        reduceDS.addSink(jdbcSink).setParallelism(1);

        env.execute("ClientSendTotalApp");

    }
}
