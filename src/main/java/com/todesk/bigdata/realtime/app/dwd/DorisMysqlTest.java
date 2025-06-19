package com.todesk.bigdata.realtime.app.dwd;


import com.ibm.icu.impl.Row;
import com.todesk.bigdata.realtime.utils.DorisUtil;
import com.todesk.bigdata.realtime.utils.EnvConfig;
import com.todesk.bigdata.realtime.utils.MySQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Author : Liuchao
 * Date : 2023/9/19 14:29
 * Desc :
 */
public class DorisMysqlTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        tableEnv.executeSql(
                "CREATE TABLE mysql_source (\n" +
                        "`id` bigint,\n"+
                        "`userid` bigint  ,\n"+
                        "`macid` string ,\n"+
                        "`order_id` bigint  ,\n"+
                        "`product_id` bigint  ,\n"+
                        "`refund_id` bigint ,\n"+
                        "`cycle_agreement_detail_id` bigint ,\n"+
                        "`renewal_times` bigint ,\n"+
                        "`desc` string  ,\n"+
                        "`upgrade_order_id` bigint  ,\n"+
                        "`amount` bigint  ,\n"+
                        "`order_type` int  ,\n"+
                        "`params` string  ,\n"+
                        "`status` int,\n"+
                        "`start_time` bigint  ,\n"+
                        "`end_time` bigint  ,\n"+
                        "`total_price` bigint  ,\n"+
                        "`actual_price` bigint  ,\n"+
                        "`total_fee` bigint ,\n"+
                        "`create_time` timestamp  ,\n"+
                        "`update_time` timestamp ,\n"+
                        "`original_id` bigint ,\n"+
                        "`charge_type` bigint ,  \n"+
                        "  primary key(id,userid,order_id,macid,product_id,order_type,charge_type,`status`,cycle_agreement_detail_id)  NOT ENFORCED " +
                        ")" + MySQLUtil.getMySQLDDL("todesk_report","tmp"));


        tableEnv.executeSql(
                "CREATE TABLE to_doris (" +
                        "`id` bigint,\n"+
                        "`userid` bigint  ,\n"+
                        "`order_id` bigint  ,\n"+
                        "`macid` string ,\n"+
                        "`product_id` bigint  ,\n"+
                        "`order_type` int  ,\n"+
                        "`charge_type` bigint ,\n"+
                        "`status` int,\n"+
                        "`cycle_agreement_detail_id` bigint ,\n"+
                        "`refund_id` bigint ,\n"+
                        "`renewal_times` bigint ,\n"+
                        "`desc` string  ,\n"+
                        "`upgrade_order_id` bigint  ,\n"+
                        "`amount` bigint  ,\n"+
                        "`params` string  ,\n"+
                        "`start_time` bigint  ,\n"+
                        "`end_time` bigint  ,\n"+
                        "`total_price` bigint  ,\n"+
                        "`actual_price` bigint  ,\n"+
                        "`total_fee` bigint ,\n"+
                        "`create_time` timestamp  ,\n"+
                        "`update_time` timestamp ,\n"+
                        "`original_id` bigint \n"+
                        ") " + DorisUtil.getDorisDDL("center.dwd_tv_bill_order_detail"));

        Table result = tableEnv.sqlQuery("SELECT id,userid,order_id,macid,product_id,order_type,charge_type,status,cycle_agreement_detail_id,refund_id,renewal_times,desc,upgrade_order_id,amount,params,start_time,end_time,total_price,actual_price,total_fee,create_time ,update_time ,original_id FROM mysql_source");

        tableEnv.createTemporaryView("sour",result);

        //tableEnv.sqlQuery("select * from  sour").execute().print();

        tableEnv.executeSql("INSERT INTO to_doris select * from sour").print();

        env.execute("DorisMysqlTest");
    }
}