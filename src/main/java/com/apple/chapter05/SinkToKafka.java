package com.apple.chapter05;

import com.apple.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author atguigu-mqx
 */
public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> result = stream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
            }
        }).map(data -> data.toString());

        result.addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "events", new SimpleStringSchema()));

        env.execute();
    }
}
