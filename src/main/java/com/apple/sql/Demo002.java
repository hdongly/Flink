package com.apple.sql;

import com.apple.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo002 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L));
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);
        Table eventTable = tblEnv.fromDataStream(eventStream, $("user"), $("url"));
//        tableEnv.createTemporaryView("EventTable", eventStream, $("timestamp").as("ts"),$("url"));
        Table visitTable = tblEnv.sqlQuery("select user, count(url) from " + eventTable + " group by user");
//        Table aliceClickTable = eventTable.where($("user").isEqual("Alice")).select($("url"), $("user"));
        tblEnv.toChangelogStream(visitTable).print();
        env.execute("Demo002");
    }
}
