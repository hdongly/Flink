package com.apple.chapter05;
import com.apple.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author atguigu-mqx
 */
public class SourceTest {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件读取
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2. 从集合读取
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 2000L));
        clicks.add(new Event("Alice", "./prod?id=100", 3000L));

        DataStreamSource<Event> stream2 = env.fromCollection(clicks);

        // 3. 从元素读取
        DataStreamSource<Event> stream3 = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 4. 从socket文本流读取
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);

        stream2.print("2");

        stream1.print("1");

        env.execute();
    }
}
