package com.todesk.bigdata.realtime.utils;

import com.todesk.bigdata.realtime.bean.BackendTotalBean;
import com.todesk.bigdata.realtime.bean.TransientSink;
import com.todesk.bigdata.realtime.common.TodeskConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;


/**
 * Author : Liuchao
 * Date : 2022/5/11 16:06
 * Desc :
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql){
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        Field[] declaredFields = obj.getClass().getDeclaredFields();
                        int skipNum = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }
                            declaredField.setAccessible(true);
                            try {
                                Object value = declaredField.get(obj);
                                preparedStatement.setObject(i + 1 - skipNum, value);
                            } catch (IllegalAccessException e) {
                                System.out.println("ClickHouse 数据插入 SQL 占位符传参异常 ~");
                                e.printStackTrace();
                            }
                        }
                    }
                },
               /* new JdbcStatementBuilder<T>(){
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        Field[] declaredFields = t.getClass().getDeclaredFields();
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            declaredField.setAccessible(true);
                            Object value = declaredField.get(t);
                            preparedStatement.setObject(i + 1,value);
                        }
                    }
                },*/
                JdbcExecutionOptions.builder()
                    .withBatchIntervalMs(0)
                    .withBatchSize(1)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withDriverName(TodeskConfig.CLICKHOUSE_DRIVER)
                    .withUrl(TodeskConfig.CLICKHOUSE_URL)
                    /*.withUsername("default")
                    .withPassword("kHpo7R9w4Dz&9Xxl")*/
                    .build()
        );
    }
}