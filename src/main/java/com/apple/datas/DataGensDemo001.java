package com.apple.datas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.junit.Test;

import java.util.Random;

public class DataGensDemo001 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

//        env
//                .addSource(new DataGeneratorSource<OrderInfo>(
//                        new RandomGenerator<OrderInfo>() {
//                            @Override
//                            public OrderInfo next() {
//                                return new OrderInfo(
//                                        random.nextInt(1, 100000),
//                                        random.nextLong(1, 1000000),
//                                        random.nextUniform(1, 1000),
//                                        System.currentTimeMillis());
//                            }
//                        }
//                ))
//                .returns(Types.POJO(OrderInfo.class))
//                .map(new MyMapFunction(5))
//                .print();

        env
                .addSource(new DataGeneratorSource<GirlInfo>(new RandomGenerator<GirlInfo>() {
                    @Override
                    public GirlInfo next() {
                        return new GirlInfo(random.nextHexString(5), random.nextInt(1, 1000));
                    }
                }))
                .returns(Types.POJO(GirlInfo.class))
                .map(new RichMapFunction<GirlInfo, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(GirlInfo value) throws Exception {
                        return Tuple2.of(value.name, 1L);
                    }
                })
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                }).print();

        env.execute();
    }

    private static class MyMapFunction implements MapFunction<OrderInfo, OrderInfo> {

        private Integer nums;

        public MyMapFunction(Integer nums) {
            this.nums = nums;
        }

        @Override
        public OrderInfo map(OrderInfo value) throws Exception {
            value.setTotal_amount(value.total_amount + nums);
            return value;
        }
    }

    @Test
    public void test01() {
        String[] girls = {"myj", "lss", "dxy", "yjy", "ly"};
        Random rand = new Random();
        for (int i = 0; i <= 10; i++) {
            System.out.println(girls[rand.nextInt(girls.length)]);
        }
    }
}
