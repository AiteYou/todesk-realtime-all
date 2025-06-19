package com.todesk.bigdata.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.BackendServBean;
import com.todesk.bigdata.realtime.bean.BackendTotalBean;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import com.todesk.bigdata.realtime.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/5/24 15:30
 * Desc : 服务器连通率情况
 */
public class BackendServApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(3, TimeUnit.SECONDS), org.apache.flink.api.common.time.Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        //System.setProperty("HADOOP_USER_NAME", "bigdata");


        String backendDetailTopic = "dwd_backend_detail";
        String groupId = "dws_backend_total_group";

        DataStreamSource<String> backentDetailSource = env.addSource(KafkaUtil.getKafkaSource(backendDetailTopic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = backentDetailSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                // null值在kafka序列化处理为""
                if (!"".equals(value)) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                }
            }
        });

        // 按照new_connid分组
        KeyedStream<JSONObject, String> connKeyedStream = jsonObjDS.keyBy(json -> json.getString("new_connid"));

        // 解决left join多条数据，获取时间取最大
        SingleOutputStreamOperator<JSONObject> backendDetailJsonObjDS = connKeyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> backendDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                backendDetailState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("backend-detail", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                // 获取状态数据并判断是否有数据
                JSONObject backendDetail = backendDetailState.value();

                if (backendDetail == null) {
                    // 把当前数据视为状态，并配置定时器
                    backendDetailState.update(value);
                    // new_connid等2s
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
                } else {
                    String stateTs = backendDetail.getString("ts");
                    String curTs = value.getString("ts");

                    int compare = TimestampLtz3CompareUtil.compare(stateTs, curTs);
                    if (compare != 1) { //表示后到的数据时间大
                        // 更新状态
                        backendDetailState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //提取状态数据并输出
                JSONObject backendDetail = backendDetailState.value();
                out.collect(backendDetail);
                backendDetailState.clear();
            }
        });

        //backendDetailJsonObjDS.print();
        SingleOutputStreamOperator<BackendServBean> backendServBeanSingleOutputStreamOperator = backendDetailJsonObjDS.map(new MapFunction<JSONObject, BackendServBean>() {
            @Override
            public BackendServBean map(JSONObject value) throws Exception {
                String handSuConnid = value.getString("hand_su_connid");

                if (handSuConnid == null) {
                    return new BackendServBean(
                            "", "", value.getString("new_serv"), 1L, 0L, 0.00, value.getLong("new_logtime")
                    );
                } else {
                    return new BackendServBean(
                            "", "", value.getString("new_serv"), 1L, 1L, 0.00, value.getLong("new_logtime")
                    );
                }
            }
        });



        SingleOutputStreamOperator<BackendServBean> jsonObjWithWatermark = backendServBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<BackendServBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<BackendServBean>() {
                            @Override
                            public long extractTimestamp(BackendServBean element, long recordTimestamp) {
                                return element.getTs();
                            }
        }));

        KeyedStream<BackendServBean, String> servBeanStringKeyedStream = jsonObjWithWatermark.keyBy(BackendServBean::getServ);

        SingleOutputStreamOperator<BackendServBean> reduceStream = servBeanStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60))).reduce(
                new ReduceFunction<BackendServBean>() {
                    @Override
                    public BackendServBean reduce(BackendServBean value1, BackendServBean value2) throws Exception {
                        value1.setNewConnectCount(value1.getNewConnectCount() + value2.getNewConnectCount());
                        value1.setHandshakeSuccessCount(value1.getHandshakeSuccessCount() + value2.getHandshakeSuccessCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<BackendServBean, BackendServBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<BackendServBean> elements, Collector<BackendServBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        DecimalFormat df = new DecimalFormat("######0.00");

                        double d = 0.0;
                        if (elements.iterator().next().getNewConnectCount() != 0) {
                            d = (double) (elements.iterator().next().getHandshakeSuccessCount()) / elements.iterator().next().getNewConnectCount() * 100;
                        } else d = 0.0;

                        String handrateStr = df.format(d);
                        Double handrateDou = Double.valueOf(handrateStr.toString());


                        //Double handrateDou = (double) (elements.iterator().next().getHandshakeSuccessCount()) / elements.iterator().next().getNewConnectCount() * 100;

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setHandshakeSuccessRate(handrateDou);
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        out.collect(elements.iterator().next());
                    }
                }
        );


        //reduceStream.print();

        SinkFunction<BackendServBean> jdbcSink = ClickHouseUtil.<BackendServBean>getJdbcSink(
                "insert into dws_backend_serv_window values(?,?,?,?,?,?,?)");


        reduceStream.addSink(jdbcSink).setParallelism(1);
        env.execute("BackendServApp");

    }
}
