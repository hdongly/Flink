package com.apple.windowing;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class AggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }))
                .keyBy(k -> "key")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AvgPv())
                .print();
        env.execute("");
    }

    private static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {

        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
            accumulator.f0.add(value.user);
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            return (double) accumulator.f1 / accumulator.f0.size();
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }
}
