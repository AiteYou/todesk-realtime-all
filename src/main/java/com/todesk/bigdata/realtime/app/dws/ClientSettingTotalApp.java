package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.ClientSettingBean;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
 * Date : 2022/6/23 19:38
 * Desc :
 */
public class ClientSettingTotalApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.SECONDS),Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        System.setProperty("HADOOP_USER_NAME", "bigdata");

        String clientConfigCenterTopic = "dwd_client_config_center_log";
        String groupId = "dws_client_setting_group";

        DataStreamSource<String> clientSettingDS = env.addSource(KafkaUtil.getKafkaSource(clientConfigCenterTopic, groupId)).setParallelism(3);
        SingleOutputStreamOperator<JSONObject> jsonObjClientSettingDS = clientSettingDS.map(JSON::parseObject).setParallelism(3);

        SingleOutputStreamOperator<ClientSettingBean> clientSettingBeanSingleOutputStreamOperator = jsonObjClientSettingDS.flatMap(
                new FlatMapFunction<JSONObject, ClientSettingBean>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<ClientSettingBean> out) throws Exception {
                        String action = value.getString("action");
                        Long ts = value.getLong("logtime");
                        if ("req".equals(action) || "resp".equals(action)) {
                            out.collect(
                                    new ClientSettingBean(
                                            "", "", action, 1L, ts
                                    )
                            );
                        }
                    }
                }
        ).setParallelism(3);

        SingleOutputStreamOperator<ClientSettingBean> jsonObjWithWatermark = clientSettingBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClientSettingBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClientSettingBean>() {
                            @Override
                            public long extractTimestamp(ClientSettingBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        KeyedStream<ClientSettingBean, String> clientSettingBeanStringKeyedStream = jsonObjWithWatermark.keyBy(ClientSettingBean::getAction);

        SingleOutputStreamOperator<ClientSettingBean> reduceDS = clientSettingBeanStringKeyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60))).reduce(
                new ReduceFunction<ClientSettingBean>() {
                    @Override
                    public ClientSettingBean reduce(ClientSettingBean value1, ClientSettingBean value2) throws Exception {
                        value1.setActionCount(value1.getActionCount() + value2.getActionCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<ClientSettingBean, ClientSettingBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<ClientSettingBean> elements, Collector<ClientSettingBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        out.collect(elements.iterator().next());
                    }
                }
        ).setParallelism(3);

        //reduceDS.print();

        SinkFunction<ClientSettingBean> jdbcSink = ClickHouseUtil.<ClientSettingBean>getJdbcSink(
                "insert into dws_client_setting_window values(?,?,?,?,?)");

        reduceDS.addSink(jdbcSink).setParallelism(1);

        env.execute("ClientSettingTotalApp");
    }
}
