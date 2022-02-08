package com.apple.chapter05;

import com.apple.bean.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TypeTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        SingleOutputStreamOperator<String> result1 = stream.map(event -> event.user);

        SingleOutputStreamOperator<String> result2 = stream.flatMap((Event event, Collector<String> out) ->
                out.collect(event.user))
//                .returns(Types.STRING);
        .returns(new TypeHint<String>() {
        });

//        SingleOutputStreamOperator<Tuple2<String, String>> result3 = stream.map(event ->
//                Tuple2.of(event.user, event.url))
//                .returns(new TypeHint<Tuple2<String, String>>() {
//                });

        SingleOutputStreamOperator<Tuple2<String, String>> result3 = stream.map(event ->
                Tuple2.of(event.user, event.url))
                .returns(Types.TUPLE(Types.STRING, Types.STRING));

        result3.print();

        env.execute();
    }
}
