package com.todesk.bigdata.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.ParserConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

/**
 * Author : Liuchao
 * Date : 2023/7/6 14:29
 * Desc :
 */
public class MyMapFunction  {
    private static final Logger log = LoggerFactory.getLogger(MyMapFunction.class);
    /*public static class PutDateFunction implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            JSONObject jsonObject = null;
            try {
                jsonObject = JSON.parseObject(value);
                String tsValue = new SimpleDateFormat("yyyy-MM-dd").format(jsonObject.getLong("logtime"));
                jsonObject.put("dt", tsValue);
            } catch (Exception e) {
                log.info("data is invalid:"+value);
                e.printStackTrace();
            }
            return jsonObject.toJSONString();
        }
    }*/
    public static class PutDateFunction implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            JSONObject jsonObject = null;
            try {
                jsonObject = JSON.parseObject(value);
                String tsValue = new SimpleDateFormat("yyyy-MM-dd").format(jsonObject.getLong("logtime"));
                jsonObject.put("dt", tsValue);
            } catch (Exception e) {
                log.info("data is invalid:"+value,e);
                return;
            }
            out.collect(jsonObject.toJSONString());
        }
    }
}
