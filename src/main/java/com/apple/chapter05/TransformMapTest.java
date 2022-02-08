package com.apple.chapter05;

import com.apple.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1. 传入lambda表达式
        SingleOutputStreamOperator<String> result1 = stream.map(data -> data.user);

        // 2. 实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MyMapper());
        result1.print("1");
        result2.print("2");

        // 3. 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result3 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        result3.print("3");

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event, String> {
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
