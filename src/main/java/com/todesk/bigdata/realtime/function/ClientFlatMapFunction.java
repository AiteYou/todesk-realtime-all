package com.todesk.bigdata.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.bean.ClientConnectLineBean;
import com.todesk.bigdata.realtime.utils.ProvinceCompareUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Author : Liuchao
 * Date : 2022/11/2 18:19
 * Desc :
 */
public class ClientFlatMapFunction implements FlatMapFunction<JSONObject, ClientConnectLineBean> {
    @Override
    public void flatMap(JSONObject value, Collector<ClientConnectLineBean> out) throws Exception {
        String line = value.getString("line");
        String newLine = "";
        String province = value.getString("province");
        boolean isProvince = ProvinceCompareUtil.compare(province);
        Long ts = value.getLong("logtime");
        if ( isProvince && line !=null ){
            if ("电信".equals(line) || "移动".equals(line) || "联通".equals(line) || "鹏博士".equals(line)){
                newLine = line;
            }else if(line.endsWith("广电")){
                newLine = "广电";
            }else if(line.contains("/")){
                newLine = line.split("/")[0];
            }else newLine= "其他";
            out.collect(
                    new ClientConnectLineBean("","",newLine,province,1L,ts)
            );
        }
    }
}