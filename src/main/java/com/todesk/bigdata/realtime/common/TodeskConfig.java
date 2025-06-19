package com.todesk.bigdata.realtime.common;

/**
 * Author : Liuchao
 * Date : 2022/5/11 10:45
 * Desc :
 */
public class TodeskConfig {
    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://113.31.158.46:8123/todesk_realtime?socket_timeout=90000";
}
