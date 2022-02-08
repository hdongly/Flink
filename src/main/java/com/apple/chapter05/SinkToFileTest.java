package com.apple.chapter05;

import com.apple.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * @author atguigu-mqx
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("UTF-8"))
                .build();

        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.toString();
            }
        }).addSink(fileSink);

        env.execute();
    }
}
