package com.apple.chapter09;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/25  9:44
 */
public class OperatorStateTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    // 自定义SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义一个批量sink的阈值
        private int threshold;

        // 定义一个列表，用于保存当前缓存的数据
        private List<Event> bufferedElements;

        // 定义一个算子状态
        private ListState<Event> operatorListState;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold){
                for (Event event: bufferedElements){
                    // 输出到外部系统
                    System.out.println(event);
                    System.out.println("-------------------------");
                }
                bufferedElements.clear();
            }
        }

        // 状态的保存
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            operatorListState.clear();
            for (Event event: bufferedElements){
                operatorListState.add(event);
            }
        }

        // 状态的恢复
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            operatorListState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Event>("buffered-list", Event.class));
            if (context.isRestored()){
                for (Event event: operatorListState.get()){
                    bufferedElements.add(event);
                }
            }
        }
    }
}
