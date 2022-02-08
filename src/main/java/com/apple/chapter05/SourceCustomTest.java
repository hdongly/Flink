package com.apple.chapter05;

import com.apple.sources.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author atguigu-mqx
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource()).print();

        env.execute();
    }
}
