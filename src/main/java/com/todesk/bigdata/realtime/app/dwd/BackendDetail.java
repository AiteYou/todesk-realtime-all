package com.todesk.bigdata.realtime.app.dwd;

import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2022/5/30 19:43
 * Desc :
 */
public class BackendDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.SECONDS),Time.of(2,TimeUnit.SECONDS)));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        //System.setProperty("HADOOP_USER_NAME", "bigdata");

        // 设定 Table 中的时区为本地时区
        //tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        //tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        // 两个流都存储3s状态，之后数据都会过期
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(3));

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("" +
                "CREATE TABLE dwd_newconnect_log_table (\n" +
                "  `connectionid` STRING,\n" +
                "  `relayservername` STRING,\n" +
                "  `srcplatform` STRING,\n" +
                "  `dstplatform` STRING,\n" +
                "  `onServer` STRING,\n" +
                "  `logtime` BIGINT,\n" +
                //"  `rt` AS TO_TIMESTAMP(FROM_UNIXTIME(logtime / 1000))\n" +
                "  `pt` AS PROCTIME()\n" +
                //"   WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_newconnect_log", "dws_backend_serv_group"));

        Table newConnTable = tableEnv.sqlQuery("" +
                "select connectionid,relayservername,srcplatform,dstplatform,onServer,logtime,pt from dwd_newconnect_log_table where connectionid is not null");

        tableEnv.createTemporaryView("newconnect_detail",newConnTable);

        tableEnv.executeSql("" +
                "CREATE TABLE dwd_handshake_success_log_table (\n" +
                "  `connectionid` STRING,\n" +
                "  `logtime` BIGINT,\n" +
                //"  `rt` AS TO_TIMESTAMP(FROM_UNIXTIME(logtime / 1000))\n" +
                "  `pt` AS PROCTIME()\n" +
                //"   WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_handshake_success_log", "dws_backend_serv_group"));

        Table handSuccessTable = tableEnv.sqlQuery("" +
                "select connectionid,logtime,pt from dwd_handshake_success_log_table");

        tableEnv.createTemporaryView("hand_success_detail",handSuccessTable);

        Table resultTable = tableEnv.sqlQuery("select " +
                "nd.connectionid,nd.relayservername,nd.srcplatform,nd.dstplatform,nd.onServer,nd.logtime,nd.pt,hd.connectionid,hd.logtime,hd.pt,current_row_timestamp() ts " +
                "from newconnect_detail nd " +
                "left join hand_success_detail hd " +
                "on nd.connectionid=hd.connectionid");

        tableEnv.createTemporaryView("result_table",resultTable);
        //tableEnv.sqlQuery("select * from newconnect_detail").execute().print();

        tableEnv.executeSql("create table dwd_backend_detail_table( " +
                " `new_connid` STRING," +
                " `new_serv` STRING," +
                " `new_su_client` STRING," +
                " `new_du_client` STRING," +
                " `new_on_server` STRING," +
                " `new_logtime`  BIGINT," +
                " `new_pt` TIMESTAMP_LTZ(3)," +
                " `hand_su_connid` STRING," +
                " `hand_su_logtime` BIGINT," +
                " `hand_su_pt` TIMESTAMP_LTZ(3)," +
                " `ts` TIMESTAMP_LTZ(3)," +
                " PRIMARY KEY (new_connid) NOT ENFORCED " +
                    ")" + KafkaUtil.getUpsertKafkaDDL("dwd_backend_detail"));
        // insert into dwd_backend_detail_table
        tableEnv.executeSql("insert into dwd_backend_detail_table select * from result_table").print();

        env.execute("BackendDetail");
    }
}
