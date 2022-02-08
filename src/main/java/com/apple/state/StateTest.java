package com.apple.state;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

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
                .flatMap(new MyFlatMapFunction())
                .print();

        env.execute();
    }

    // 自定义实现RichFlatMapFunction
    public static class MyFlatMapFunction extends RichFlatMapFunction<Event, String>{
        // 定义各种状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggState;

        // 定义一个属性
        private Long count = 0L;

        // 在open生命周期方法中获取状态的句柄
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<Event>("my-value", Event.class);
            myValueState = getRuntimeContext().getState(valueStateDescriptor);

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Types.POJO(Event.class)));

            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", Types.STRING, Types.LONG));

            // 聚合状态的写法
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.user, value1.url, value2.timestamp);
                        }
                    },
                    Event.class
            ));

            AggregatingStateDescriptor<Event, Long, String> aggregatingStateDescriptor = new AggregatingStateDescriptor<Event, Long, String>(
                    "my-agg",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event value, Long accumulator) {
                            return accumulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    },
                    Long.class
            );

            myAggState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);

            // 配置状态的TTL
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            aggregatingStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 使用值状态
            myValueState.update(value);
            System.out.println("ValueState: " + myValueState.value());

            // 使用列表状态
            myListState.add(value);
            myListState.get();

            // 使用映射状态
            if (myMapState.contains(value.user)){
                System.out.println("MapState: " + myMapState.get(value.user));
            }
            myMapState.put(value.user, 1L);

            myReducingState.add(value);
            System.out.println("Reducing State: " + myReducingState.get());

            myAggState.add(value);
            System.out.println("Aggregating State: " + myAggState.get());

            // 改变本地变量
            count ++;
            System.out.println("Count: " + count);

            System.out.println("---------------------------------");
        }
    }
}
