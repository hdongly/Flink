package com.apple;

import com.apple.bean.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author atguigu-mqx
 */
public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        stream.map(new RichMapFunction<Event, Integer>() {
            @Override
            public Integer map(Event value) throws Exception {
                return value.url.length();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "号任务 open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "号任务 close");
            }
        })
                .print();

        env.execute();

    }
}
