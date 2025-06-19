package com.todesk.bigdata.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author : Liuchao
 * Date : 2023/7/6 15:46
 * Desc :
 */
public class MyProcessFunction {
    public static class LogTypeFlagFunction extends ProcessFunction<String, String>{
        @Override
        public void processElement(String jsonStr, Context ctx, Collector<String> out) throws Exception {
            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
            String logtypeStr = jsonObj.getString("logtype");
            ctx.output(new OutputTag<String>(logtypeStr){},jsonStr);
        }
    }
}
