package com.apple.chapter05;

import com.apple.bean.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1. lambda表达式
        stream.filter(data -> data.user.equals("Mary")).print("lambda");

        // 2. 实现FilterFunction接口
        stream.filter(new MyFilter()).print("filter function");

        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Mary");
            }
        }).print();

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event>{
        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}
