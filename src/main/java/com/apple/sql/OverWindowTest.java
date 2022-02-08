package com.apple.sql;

import com.apple.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class OverWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 流转成表的过程中定义时间属性字段
        SingleOutputStreamOperator<Event> eventStream = env.readTextFile("input/clicks.txt")
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        Table eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("rt").rowtime());
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 开窗聚合
        Table result = tableEnv.sqlQuery("select user, " +
                "AVG(ts) OVER (" +
                "  PARTITION BY user " +
                "  ORDER BY rt " +
                "  ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
                ")" +
                "FROM EventTable");

        // 创建连接器表，在控制台打印输出
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "avg_ts BIGINT )" +
                "WITH (" +
                "'connector' = 'print'" +
                ")");
        result.executeInsert("output");
    }
}
