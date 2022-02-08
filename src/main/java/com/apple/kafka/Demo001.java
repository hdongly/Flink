package com.apple.kafka;

import com.apple.bean.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class Demo001 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> eventDS = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L));
        eventDS
//                .map(new ChangeURLMapFunction()) //change url
//                .filter(new FilterMaryFilterFunction())
//                .keyBy(e -> e.user)
                .keyBy(new KeybyUser())
                .rescale()
                .print();
        env.execute();
    }

    private static class ChangeURLMapFunction implements MapFunction<Event, Event> {
        @Override
        public Event map(Event value) throws Exception {
            value.setUrl("./google");
            return value;
        }
    }

    private static class FilterMaryFilterFunction implements FilterFunction<Event> {

        @Override
        public boolean filter(Event value) throws Exception {
            return "Mary".equals(value.user);
        }
    }

    private static class KeybyUser implements KeySelector<Event, String> {

        @Override
        public String getKey(Event value) throws Exception {
            return value.user;
        }
    }
}
