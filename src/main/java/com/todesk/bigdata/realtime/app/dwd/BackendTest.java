package com.todesk.bigdata.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Author : Liuchao
 * Date : 2022/5/31 11:06
 * Desc :
 */
public class BackendTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度，和kafka分区数保持一致
        env.setParallelism(3);

        String newConnectTopic = "dwd_newconnect_log";
        String groupId = "dws_backend_serv_group";
        DataStreamSource<String> backendNewConnectSource = env.addSource(KafkaUtil.getKafkaSource(newConnectTopic, groupId));
        SingleOutputStreamOperator<JSONObject> backendNewConnectStream = backendNewConnectSource.map(JSON::parseObject);

        String handshakeSuccessTopic = "dwd_handshake_success_log";
        DataStreamSource<String> handshakeSuccessSource = env.addSource(KafkaUtil.getKafkaSource(handshakeSuccessTopic, groupId));
        SingleOutputStreamOperator<JSONObject> handshakeSuccessStream = handshakeSuccessSource.map(JSON::parseObject);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("newconnect_detail",backendNewConnectStream);
        tableEnv.createTemporaryView("hand_success_detail",handshakeSuccessStream);

       /* Table resultTable = tableEnv.sqlQuery("select " +
                "nd.connectionid,nd.relayservername,nd.logtime,nd.pt,hd.connectionid,hd.logtime,hd.pt,current_row_timestamp() ts " +
                "from newconnect_detail nd " +
                "left join hand_success_detail hd " +
                "on nd.connectionid=hd.connectionid");

        DataStream<Tuple2<Boolean, Row>> joinDS = tableEnv.toRetractStream(resultTable, Row.class);
        //撤回流中如果是更新的数据 会出现两条数据， false表示更新前， true表示更新后， 此处只要最新数据
        SingleOutputStreamOperator<Row> map = joinDS.filter(data -> data.f0).map(data -> data.f1);*/

        tableEnv.sqlQuery("select * from newconnect_detail").execute().print();
        env.execute();
    }
}
