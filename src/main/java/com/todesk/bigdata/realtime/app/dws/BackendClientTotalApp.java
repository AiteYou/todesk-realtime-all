package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.BackendClientBean;
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
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/6/15 16:30
 * Desc :
 */
public class BackendClientTotalApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         //Configuration conf = new Configuration();
        //conf.getString(RestOptions.BIND_PORT, "8082");
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(3, TimeUnit.SECONDS), org.apache.flink.api.common.time.Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        System.setProperty("HADOOP_USER_NAME", "bigdata");

        String backendDetailTopic = "dwd_backend_detail";
        String groupId = "dws_backend_client_group";
        DataStreamSource<String> backentDetailSource = env.addSource(KafkaUtil.getKafkaSource(backendDetailTopic, groupId)).setParallelism(3);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = backentDetailSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                // null值在kafka序列化处理为""
                if (!"".equals(value)) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                }
            }
        }).setParallelism(3);

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
        }).setParallelism(3);



        SingleOutputStreamOperator<BackendClientBean> backendClientBeanSingleOutputStreamOperator = backendDetailJsonObjDS.map(
                new MapFunction<JSONObject, BackendClientBean>() {
                    @Override
                    public BackendClientBean map(JSONObject value) throws Exception {
                        String handSuConnid = value.getString("hand_su_connid");

                        if (handSuConnid == null) {
                            return new BackendClientBean(
                                    "", "", value.getString("new_su_client"), value.getString("new_du_client"), 1L, 0L, 0.00, value.getLong("new_logtime")
                            );
                        } else {
                            return new BackendClientBean(
                                    "", "", value.getString("new_su_client"), value.getString("new_du_client"), 1L, 1L, 0.00, value.getLong("new_logtime")
                            );
                        }
                    }
                }
        ).setParallelism(3);


        SingleOutputStreamOperator<BackendClientBean> jsonObjWithWatermark = backendClientBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<BackendClientBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<BackendClientBean>() {
                            @Override
                            public long extractTimestamp(BackendClientBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        KeyedStream<BackendClientBean, String> backendClientBeanStringKeyedStream = jsonObjWithWatermark.keyBy(data -> (data.getSuClient()) + data.getDuClient());


        SingleOutputStreamOperator<BackendClientBean> reduceDS = backendClientBeanStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(60))).reduce(
                new ReduceFunction<BackendClientBean>() {
                    @Override
                    public BackendClientBean reduce(BackendClientBean value1, BackendClientBean value2) throws Exception {
                        value1.setNewConnectCount(value1.getNewConnectCount() + value2.getNewConnectCount());
                        value1.setHandshakeSuccessCount(value1.getHandshakeSuccessCount() + value2.getHandshakeSuccessCount());
                        return value1;
                    }
                },
                new ProcessWindowFunction<BackendClientBean, BackendClientBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<BackendClientBean> elements, Collector<BackendClientBean> out) throws Exception {
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
        ).setParallelism(3);

        //reduceDS.print().setParallelism(1);
        SinkFunction<BackendClientBean> jdbcSink = ClickHouseUtil.<BackendClientBean>getJdbcSink("insert into dws_backend_client_total_window values(?,?,?,?,?,?,?,?)");
        reduceDS.addSink(jdbcSink).setParallelism(1);

        env.execute("BackendClientTotalApp");
    }
}
