package com.apple.chapter09;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/24  14:59
 */
public class FakeWindowExample {
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

        // 开窗统计每10秒内，每个url的访问量
        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10 * 1000L))
                .print();

        env.execute();
    }

    // 自定义实现KeyedProcessFunction，模拟滚动窗口
    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String>{
        // 定义属性，窗口长度
        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 定义MapState，用来保存每个窗口对应的访问量（windowStart，count）
        MapState<Long, Long> windowCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，按照时间戳分配窗口
            Long windowStart = value.timestamp / windowSize * windowSize;
//            Long windowStart = value.timestamp - value.timestamp % windowSize;

            Long windowEnd = windowStart + windowSize;
            // 注册end -1 定时器，触发窗口计算
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            // 增量聚合，更新状态
            if (windowCountMapState.contains(windowStart)){
                // 如果存在key，取原先的值加一，并更新状态
                Long count = windowCountMapState.get(windowStart);
                windowCountMapState.put(windowStart, count + 1);
            } else {
                windowCountMapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出结果
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = windowCountMapState.get(windowStart);
            String url = ctx.getCurrentKey();
            out.collect("url: " + url
                    + " 访问量：" + count
                    + " 窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
            );

            // 模拟窗口的销毁
            windowCountMapState.remove(windowStart);
        }
    }
}
