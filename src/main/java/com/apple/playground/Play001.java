package com.apple.playground;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class Play001 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        SingleOutputStreamOperator<Girl> ageStream = dataStream.flatMap(new FlatMapFunction<Girl, Girl>() {
//            @Override
//            public void flatMap(Girl value, Collector<Girl> out) throws Exception {
//                value.age++;
//                out.collect(value);
//            }
//        });
        DataStream<Girl> dataStream = env
                .fromElements(new Girl("myj", 18, 2100L), new Girl("lss", 20, 3000L), new Girl("dxy", 18, 1990L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Girl>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Girl>() {
                    @Override
                    public long extractTimestamp(Girl element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Girl>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Girl>() {
//                    @Override
//                    public long extractTimestamp(Girl element, long recordTimestamp) {
//                        return element.timeStamp;
//                    }
//                }));
        SingleOutputStreamOperator<Girl> ageStream = dataStream.flatMap(new MinusAge(3));

        ageStream.print();

        env.execute();
    }

    protected static class AddAge implements FlatMapFunction<Girl, Girl> {
        private int nums = 10;

        public AddAge() {

        }

        public AddAge(int nums) {
            this.nums = nums;
        }

        @Override
        public void flatMap(Girl value, Collector out) {
            value.age += nums;
            out.collect(value);
        }
    }

    protected static class FilterMYJ implements FlatMapFunction<Girl, Girl> {

        @Override
        public void flatMap(Girl value, Collector out) {
            if (value.name.equals("myj")) {
                out.collect(value);
            }
        }
    }

    protected static class MinusAge extends RichFlatMapFunction<Girl, Girl> {
        private int nums;

        public MinusAge() {

        }

        public MinusAge(int nums) {
            this.nums = nums;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open");
        }

        @Override
        public void flatMap(Girl value, Collector<Girl> out) throws Exception {
            value.age -= nums;
            out.collect(value);
        }

        @Override
        public void close() throws Exception {
            System.out.println("Close");
        }
    }
}
