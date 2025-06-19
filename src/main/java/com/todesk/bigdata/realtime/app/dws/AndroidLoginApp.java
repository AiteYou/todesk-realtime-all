package com.todesk.bigdata.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.AndroidLoginBean;
import com.todesk.bigdata.realtime.utils.ClickHouseUtil;
import com.todesk.bigdata.realtime.utils.DateFormatUtil;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/7/10 16:07
 * Desc :
 */
public class AndroidLoginApp {
    public static final Logger log = LoggerFactory.getLogger(AndroidLoginApp.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.of(3, TimeUnit.SECONDS), org.apache.flink.api.common.time.Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());

       env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
//        System.setProperty("HADOOP_USER_NAME", "bigdata");

        String logicTopic = "dwd_click_login_log";
        String groupId = "dws_android_login_group";
        //String groupId = "test";

        DataStreamSource<String> logicDS = env.addSource(KafkaUtil.getKafkaSource(logicTopic, groupId)).setParallelism(3);

        SingleOutputStreamOperator<JSONObject> loginJsonDS = logicDS.map(JSON::parseObject).setParallelism(3);

        //loginJsonDS.print();

        SingleOutputStreamOperator<AndroidLoginBean> AndroidLoginBeanSingleOutputStreamOperator = loginJsonDS.flatMap(
                new FlatMapFunction<JSONObject, AndroidLoginBean>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<AndroidLoginBean> out) throws Exception {
                        String action = value.getString("action");
                        String platform = value.getString("platform");
                        String version = value.getString("version");
                        String ckUrlStr = value.getString("ck_url");

                        // 拿到登录相关数据
                        if (ckUrlStr !=null && ckUrlStr.contains("{") && ckUrlStr.contains("}")) {
                            JSONObject ckUrl = JSONObject.parseObject(ckUrlStr);
                            String loginType = ckUrl.getString("login_type");
                            String autoLogin = ckUrl.getString("auto_login");
                            String resultCode = ckUrl.getString("result_code");
                            String loginStep = ckUrl.getString("login_step");
                            Long ts = value.getLong("logtime");
                            //System.out.println(value+"loginType:"+loginType+"autoLogin:"+autoLogin+"resultCode:"+resultCode+"loginStep:"+loginStep);
                            // android登录处理
                            if ("android".equals(platform)) {
                                if (version.compareTo("4.4.3.3") >= 0 && loginType != null && autoLogin != null ) {
                                    // 登录发起数
                                    if ("signin_req".equals(action) && "step2".equals(loginStep)) {
                                        // 自动登录
                                        if ("1".equals(autoLogin)) {
                                            // 账号自动登录发起
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "accountAuto", 0L, 1L, 0.00, ts)
                                                    );
                                                    // 微信自动登录发起
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "wechatAuto", 0L, 1L, 0.00, ts)
                                                    );
                                                    // 验证码自动登录发起
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "", platform, version, "phoneAuto", 0L, 1L, 0.00, ts)
                                                    );
                                                    break;
                                            }
                                        // 手动登录
                                        }else if ("2".equals(autoLogin)){
                                            // 账号手动登录发起
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "", platform, version, "accountLogin", 0L, 1L, 0.00, ts)
                                                    );
                                                    //System.out.println("账号发起"+value);
                                                    // 微信手动登录发起
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "", platform, version, "wechatLogin", 0L, 1L, 0.00, ts)
                                                    );
                                                    // 验证码手动登录发起
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "", platform, version, "phoneLogin", 0L, 1L, 0.00, ts)
                                                    );
                                                    //System.out.println("验证码发起"+value);
                                                    break;
                                            }
                                        }
                                    // 登录成功数
                                    }else if ("signin".equals(action) && "1".equals(resultCode)){
                                        // 自动登录
                                        if ("1".equals(autoLogin)) {
                                            // 账号自动登录成功
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "accountAuto", 1L, 0L, 0.00, ts)
                                                    );
                                                    // 微信自动登录成功
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "wechatAuto", 1L, 0L, 0.00, ts)
                                                    );
                                                    // 验证码自动登录成功
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "phoneAuto", 1L, 0L, 0.00, ts)
                                                    );
                                                    break;
                                            }
                                            // 手动登录
                                        }else if ("2".equals(autoLogin)){
                                            // 账号手动登录成功
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "accountLogin", 1L, 0L, 0.00, ts)
                                                    );
                                                    //System.out.println("账号成功"+value);
                                                    // 微信手动登录成功
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "wechatLogin", 1L, 0L, 0.00, ts)
                                                    );
                                                    // 验证码手动登录成功
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "phoneLogin", 1L, 0L, 0.00, ts)
                                                    );
                                                    //System.out.println("验证码成功"+value);
                                                    break;
                                            }
                                        }
                                    }
                                }
                            // iOS登录处理
                            }else if ("iOS".equals(platform)){
                                if (version.compareTo("4.4.3") >= 0  && loginType != null && autoLogin != null ){
                                    // 登录发起数
                                    if ("signin_req".equals(action) && "step2".equals(loginStep)) {
                                        // 自动登录
                                        if ("1".equals(autoLogin)) {
                                            // 账号自动登录发起
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "accountAuto", 0L, 1L, 0.00, ts)
                                                    );
                                                    // 微信自动登录发起
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "wechatAuto", 0L, 1L, 0.00, ts)
                                                    );
                                                    // 验证码自动登录发起
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "phoneAuto", 0L, 1L, 0.00, ts)
                                                    );
                                                    break;
                                                case "4":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "appleAuto", 0L, 1L, 0.00, ts)
                                                    );
                                                    break;
                                            }
                                            // 手动登录
                                        }else if ("2".equals(autoLogin)){
                                            // 账号手动登录发起
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "accountLogin", 0L, 1L, 0.00, ts)
                                                    );
                                                    // 微信手动登录发起
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "wechatLogin", 0L, 1L, 0.00, ts)
                                                    );
                                                    // 验证码手动登录发起
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "phoneLogin", 0L, 1L, 0.00, ts)
                                                    );
                                                    break;
                                                case "4":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "appleLogin", 0L, 1L, 0.00, ts)
                                                    );
                                                    break;
                                            }
                                        }
                                        // 登录成功数
                                    }else if ("signin".equals(action) && "1".equals(resultCode)){
                                        // 自动登录
                                        if ("1".equals(autoLogin)) {
                                            // 账号自动登录成功
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "", platform, version, "accountAuto", 1L, 0L, 0.00, ts)
                                                    );
                                                    // 微信自动登录成功
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "wechatAuto", 1L, 0L, 0.00, ts)
                                                    );
                                                    // 验证码自动登录成功
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "phoneAuto", 1L, 0L, 0.00, ts)
                                                    );
                                                    break;
                                                case "4":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "appleAuto", 1L, 0L, 0.00, ts)
                                                    );
                                                    break;
                                            }
                                            // 手动登录
                                        }else if ("2".equals(autoLogin)){
                                            // 账号手动登录成功
                                            switch (loginType) {
                                                case "0":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "accountLogin", 1L, 0L, 0.00, ts)
                                                    );
                                                    // 微信手动登录成功
                                                    break;
                                                case "1":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "wechatLogin", 1L, 0L, 0.00, ts)
                                                    );
                                                    // 验证码手动登录成功
                                                    break;
                                                case "2":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "phoneLogin", 1L, 0L, 0.00, ts)
                                                    );
                                                    break;
                                                case "4":
                                                    out.collect(
                                                            new AndroidLoginBean("", "",  platform, version, "appleLogin", 1L, 0L, 0.00, ts)
                                                    );
                                                    break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
        ).setParallelism(3);

         //AndroidLoginBeanSingleOutputStreamOperator.print();

        SingleOutputStreamOperator<AndroidLoginBean> androidLoginETLWithWatermark = AndroidLoginBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AndroidLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<AndroidLoginBean>() {
                            @Override
                            public long extractTimestamp(AndroidLoginBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );


        KeyedStream<AndroidLoginBean, String> keyedDS = androidLoginETLWithWatermark.keyBy(data ->  (data.getPlatform()+data.getVersion()+data.getType()));


        //keyedDS.print("<<<");
        SingleOutputStreamOperator<AndroidLoginBean> reduceDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(60))).reduce(
                new ReduceFunction<AndroidLoginBean>() {
                    @Override
                    public AndroidLoginBean reduce(AndroidLoginBean value1, AndroidLoginBean value2) throws Exception {
                        value1.setSuccessCount(value1.getSuccessCount() + value2.getSuccessCount());
                        value1.setTotalCount(value1.getTotalCount() + value2.getTotalCount());
                        //System.out.println(value1);
                        return value1;
                    }
                },
                new ProcessWindowFunction<AndroidLoginBean, AndroidLoginBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<AndroidLoginBean> elements, Collector<AndroidLoginBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        DecimalFormat df = new DecimalFormat("######0.00");

                        double d = 0.0;
                        if (elements.iterator().next().getTotalCount() != 0) {
                            d = (double) (elements.iterator().next().getSuccessCount()) / elements.iterator().next().getTotalCount() * 100;
                            //System.out.println(d);
                        } else d = 0.0;

                        String handrateStr = df.format(d);
                        Double handrateDou = Double.valueOf(handrateStr.toString());

                        //System.out.println(elements.iterator().next());

                        elements.iterator().next().setStt(stt);
                        elements.iterator().next().setEdt(edt);
                        elements.iterator().next().setSuccessRate(handrateDou);
                        elements.iterator().next().setTs(System.currentTimeMillis());
                        //System.out.println("cccc"+elements.iterator().next());
                        out.collect(elements.iterator().next());

                    }
                }
        ).setParallelism(3);

        SinkFunction<AndroidLoginBean> jdbcSink = ClickHouseUtil.<AndroidLoginBean>getJdbcSink(
                "insert into dws_android_login_window values(?,?,?,?,?,?,?,?,?)");

        reduceDS.addSink(jdbcSink).setParallelism(1);

        env.execute("AndroidLoginApp");
    }
}
