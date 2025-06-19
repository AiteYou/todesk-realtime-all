package com.todesk.bigdata.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Author : LiuChao
 * Date : 2022/5/3 17:45
 * Desc : 操作Kafka工具类
 */
public class KafkaUtil {
    //private static final String KAFKA_SERVER = "172.16.1.2:9093,172.16.1.3:9093,172.16.1.1:9093";
    private static final String KAFKA_SERVER = "zuler102:9092,zuler101:9092,zuler103:9092";

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"1");
        //props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"2000");
        //props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"1048576");

        // 查看kafka序列化，new SimpleStringSchema时是非null会抛异常，须自定义反序列化
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    // 是否是最后一个
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if ( record == null || record.value() == null){
                            return "";
                        } else {
                            return new String(record.value());
                        }
                    }

                    @Override
                    // 返回string
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                props);
        return consumer;
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        //prop.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"2097152");
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"3145728"); //3M
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG,"100"); // 100ms或者batch满1M往kafka发一次数据
        prop.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"10485760"); //允许生产端发送的最大消息
        prop.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"134217728"); //128M

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), prop);
        return producer;

    }

    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset')";
    }


    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'properties.max.request.size' = ' 2097152 ',"  +
                "  'properties.batch.size' = ' 1048576 ',"  +
                "  'properties.linger.ms' = ' 100 ',"  +
                "  'properties.buffer.memory' = ' 134217728 ',"  +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}
