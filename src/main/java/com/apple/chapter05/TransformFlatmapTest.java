package com.apple.chapter05;
import com.apple.bean.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatmapTest {
    public static void main(String[] args) throws Exception {
            // 创建执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                    new Event("Bob", "./cart", 2000L),
                    new Event("Alice", "./prod?id=100", 3000L)
            );

        // 1. lambda表达式
        stream.flatMap( ((Event value, Collector<String> out) -> {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }) ).returns(new TypeHint<String>() {
        }).print("lambda");

        // 2. 实现FlatMapFunction接口
        stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).print("flatmap function");

        env.execute();

    }
}
