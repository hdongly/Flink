package com.apple.state;

import com.apple.bean.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MyFlatmapFunction extends RichFlatMapFunction<Event, String> {
    ValueState<Event> valueState;
    ListState<Event> listState;
    MapState<String, Long> mapState;
    ReducingState<Event> reducingState;
    AggregatingState<Event, String> aggregatingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<Event>("my_value_state", Event.class);
        valueState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void flatMap(Event event, Collector<String> collector) throws Exception {
        valueState.update(event);
        System.out.println(valueState.value());
    }
}
