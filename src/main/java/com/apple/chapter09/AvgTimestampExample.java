package com.apple.chapter09;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/24  15:31
 */
public class AvgTimestampExample {
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

        // 统计用户最近5次点击数据的平均时间戳
        stream.keyBy(data -> data.user)
                .flatMap(new AvgTsResult())
                .print();

        env.execute();
    }

    // 自定义实现RichFlatMapFunction
    public static class AvgTsResult extends RichFlatMapFunction<Event, String>{
        // 定义状态保存当前用户访问频次
        ValueState<Long> countState;

        // 全窗口聚合
        // 定义一个ListState保存所有数据的时间戳
        ListState<Long> tsListState;

        // 增量聚合
        // 用一个AggregatingState保存中间聚合状态
        AggregatingState<Event, Long> avgTsAggState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            tsListState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("ts-list", Long.class));

            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));
        }

//        @Override
//        public void flatMap(Event value, Collector<String> out) throws Exception {
//            // 获取当前的状态
//            Long count = countState.value();
//            if (count == null){
//                count = 1L;
//            } else {
//                count ++;
//            }
//
//            // 更新状态
//            countState.update(count);
//            tsListState.add(value.timestamp);
//
//            // 如果达到5次，计算输出
//            if (count == 5){
//                Long sum = 0L;
//                for (Long ts: tsListState.get()){
//                    sum += ts;
//                }
//                out.collect(value.user + "过去5次访问平均时间戳：" + new Timestamp(sum / 5));
//                // 清理状态
//                countState.clear();
//                tsListState.clear();
//            }
//        }


        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 获取当前的状态
            Long count = countState.value();
            if (count == null){
                count = 1L;
            } else {
                count ++;
            }

            // 更新状态
            countState.update(count);
            avgTsAggState.add(value);

            // 达到5次就输出
            if (count == 5){
                out.collect(value.user + "过去5次访问平均时间戳：" + new Timestamp(avgTsAggState.get()));
                // 清理状态
                countState.clear();
                avgTsAggState.clear();
            }
        }
    }
}
