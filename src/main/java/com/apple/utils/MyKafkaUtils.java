package com.apple.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtils {
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }
}
