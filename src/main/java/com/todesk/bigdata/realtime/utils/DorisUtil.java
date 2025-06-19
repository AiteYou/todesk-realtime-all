package com.todesk.bigdata.realtime.utils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * Author : Liuchao
 * Date : 2023/7/5 15:12
 * Desc :
 */
public class DorisUtil {

    public static SinkFunction<String> getDorisSink(String tableName,int batchSize){

       /* SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String formatDate = sdf.format(new Date());
        String label = String.format("flink_connector_%s_%s_%s", formatDate,tableName.split("\\.")[1],
        UUID.randomUUID().toString().replaceAll("-", ""));*/

        // 此版本没有给label加前缀功能，尝试自定义格式失败
        Properties properties = new Properties();
        properties.setProperty("format","json");
        properties.setProperty("strip_outer_array","true");
        properties.setProperty("max_filter_ratio","0.2");
        properties.setProperty("read_json_by_line","true");
        //properties.setProperty("label",label);

        return DorisSink.sink(
                new DorisExecutionOptions.Builder()
                        .setBatchSize(batchSize * 1024 * 1024)
                        .setBatchIntervalMs(10 * 1000L)
                        .setMaxRetries(3)
                        .setStreamLoadProp(properties)
                        .build(),
                new DorisOptions.Builder()
                        .setFenodes("192.168.250.45:8028")
                        .setUsername("root")
                        .setPassword("7acPRJOuemcLv^7t")
                        .setTableIdentifier(tableName)
                        .build()
        );
    }

    public static String getDorisDDL(String tabName) {
        return  " WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '192.168.250.45:8028',\n" +
                "  'table.identifier' = '" + tabName + "',\n" +
                "  'sink.batch.size' = '100',\n" +
                "  'sink.batch.interval' = '60',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '7acPRJOuemcLv^7t'\n" +
                ")";
    }
}