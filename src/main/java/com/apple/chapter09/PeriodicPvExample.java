package com.apple.chapter09;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/24  14:15
 */
public class PeriodicPvExample {
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

        stream.keyBy(data -> data.user)
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    // 自定义KeyedProcessFunction
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String>{
        // 定义值状态，保存当前pv的计数，定时器时间戳
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，就更新count值
            Long count = countState.value();
            if (count == null){
                countState.update(1L);
            } else {
                countState.update(count + 1);
            }
            // 注册定时器
            if (timerTsState.value() == null) {
                Long ts = value.timestamp + 10 * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出结果
            out.collect("用户" + ctx.getCurrentKey() + "的pv值：" + countState.value());
            // 清理状态
            timerTsState.clear();
//            countState.clear();
        }
    }
}
