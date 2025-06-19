package com.todesk.bigdata.realtime.utils;

/**
 * Author : Liuchao
 * Date : 2023/9/20 15:23
 * Desc :
 */
public class MySQLUtil {
    public static String getMySQLDDL(String dbName, String tabName) {

        return " WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                        //"  'hostname' = 'gz-cdbrg-frwg2xfv.sql.tencentcdb.com',\n" +
                        "  'hostname' = '192.168.250.46',\n" +
                        "  'port' = '3306',\n" +
                        "  'username' = 'root',\n" +
                        //"  'password' = 'fya12345FYD',\n" +
                        "  'password' = 'wjhwdbRM7E&E^cy^',\n" +
                        "  'database-name' = '" + dbName + "',\n" +
                        "  'table-name' =  '"+ tabName + "',\n" +
                        "  'scan.incremental.snapshot.enabled' = 'false'\n" +
                        ")";
    }
}
