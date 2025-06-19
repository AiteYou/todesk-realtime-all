package com.todesk.bigdata.realtime.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * Author : Liuchao
 * Date : 2023/5/23 18:04
 * Desc :
 */
public class EnvConfig {
    public static void configureExecutionEnvironment(StreamExecutionEnvironment env) {
        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));
//        env.getCheckpointConfig().setCheckpointTimeout(120000L);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster-hdfs/realtime/checkpoint");
        env.disableOperatorChaining();
    }
}
