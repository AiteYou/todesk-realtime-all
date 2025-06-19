package com.todesk.bigdata.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.todesk.bigdata.realtime.function.MyMapFunction;
import com.todesk.bigdata.realtime.function.MyProcessFunction;
import com.todesk.bigdata.realtime.utils.DorisUtil;
import com.todesk.bigdata.realtime.utils.EnvConfig;
import com.todesk.bigdata.realtime.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * Author : Liuchao
 * Date : 2023/7/5 17:48
 * Desc :
 */
public class DorisBackendDetailApp {
    private static final Map<OutputTag<String>, String> tagTableMapping = new HashMap<>();
    static {
        tagTableMapping.put(new OutputTag<String>("newconnect"){}, "todesk.dwd_new_connect");
        tagTableMapping.put(new OutputTag<String>("handshake_success"){}, "todesk.dwd_handshake_success");
        tagTableMapping.put(new OutputTag<String>("authentication_success"){}, "todesk.dwd_authentication_success");
        tagTableMapping.put(new OutputTag<String>("connection_stat"){}, "todesk.dwd_session_success");
        tagTableMapping.put(new OutputTag<String>("connection_closed"){}, "todesk.dwd_connection_closed");
        tagTableMapping.put(new OutputTag<String>("controller_not_present"){}, "todesk.dwd_controller_lost");
        tagTableMapping.put(new OutputTag<String>("presenter_not_present"){}, "todesk.dwd_presenter_lost");
        tagTableMapping.put(new OutputTag<String>("newuser"){}, "todesk.dwd_new_user");
        tagTableMapping.put(new OutputTag<String>("add_device"){}, "todesk.dwd_add_device");
        tagTableMapping.put(new OutputTag<String>("delete_device"){}, "todesk.dwd_delete_device");
        tagTableMapping.put(new OutputTag<String>("add_group"){}, "todesk.dwd_add_group");
        tagTableMapping.put(new OutputTag<String>("delete_group"){}, "todesk.dwd_delete_group");
        tagTableMapping.put(new OutputTag<String>("del_fserver"){}, "todesk.dwd_del_fserver");
        tagTableMapping.put(new OutputTag<String>("Connect_Limit_Sline"){}, "todesk.dwd_connect_limit_sline");
        tagTableMapping.put(new OutputTag<String>("passtimelock"){}, "todesk.dwd_passtime_lock");
        tagTableMapping.put(new OutputTag<String>("newfileconnect"){}, "todesk.dwd_newfile_connect");
        tagTableMapping.put(new OutputTag<String>("newfileconnect_fail"){}, "todesk.dwd_newfileconnect_fail");
        tagTableMapping.put(new OutputTag<String>("send_cmd"){}, "todesk.dwd_send_cmd");
        tagTableMapping.put(new OutputTag<String>("send_cmd_fail"){}, "todesk.dwd_send_cmd_fail");
        tagTableMapping.put(new OutputTag<String>("control_remote_camera"){}, "todesk.dwd_control_remote_camera");
        tagTableMapping.put(new OutputTag<String>("control_remote_camera_fail"){}, "todesk.dwd_control_remote_camera_fail");
        tagTableMapping.put(new OutputTag<String>("connect_limit"){}, "todesk.dwd_connect_limit");
        tagTableMapping.put(new OutputTag<String>("freepassword"){}, "todesk.dwd_free_password");
        tagTableMapping.put(new OutputTag<String>("ban_P1"){}, "todesk.dwd_ban_p1");
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvConfig.configureExecutionEnvironment(env);
        //env.disableOperatorChaining();

        String topic = "doris_dwd_backend_log";
        String groupId = "kafka_to_doris";

        //FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        FlinkKafkaConsumer<String> kafkaSource = (FlinkKafkaConsumer<String>) KafkaUtil.getKafkaSource(topic, groupId).setStartFromLatest();
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource).setParallelism(3);
        SingleOutputStreamOperator<String> resultDS = kafkaDS.process(new MyProcessFunction.LogTypeFlagFunction()).setParallelism(3);

        List<OutputTag<String>> outputTags = new ArrayList<>(tagTableMapping.keySet());

        for (OutputTag<String> tag : outputTags) {
            resultDS.getSideOutput(tag).addSink(DorisUtil.getDorisSink(tagTableMapping.get(tag),10)).name(tag.getId()).setParallelism(1);
        }

        env.execute("DorisBackendDetailApp");
    }
}
