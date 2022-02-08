package com.apple.sql;

import com.apple.sources.ClickSource;
import com.apple.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/26  10:14
 */
public class SQLCommonAPITest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境 - 1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        StreamTableEnvironment tableEnv1 = StreamTableEnvironment.create(env, settings1);


        // 2
        EnvironmentSettings settings2 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);

        // 将流转换成表
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        eventStream.print("input");

        tableEnv.executeSql("create table clicks (" +
                "`user` STRING, " +
                "url STRING, " +
                "ts BIGINT ) " +
                "WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' = 'csv'" +
                ")"
        );

//        Table eventTable = tableEnv.fromDataStream(eventStream, $("url"), $("timestamp").as("ts"));
        Table eventTable = tableEnv.fromDataStream(eventStream);
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 查询表
        // Table API
        Table resultTable = eventTable.select($("url"), $("user"))
                .where($("user").isEqual("Mary"));

        // SQL
        Table resultSQLTable = tableEnv.sqlQuery("select url, user from clicks where user = 'Bob'");

        // 聚合统计
        Table aggTable = tableEnv.sqlQuery("select user, count(*) from EventTable group by user");

//        tableEnv.toDataStream(resultTable)
//                .print("result");
//        tableEnv.toDataStream(resultSQLTable)
//                .print("result SQL");

        tableEnv.toChangelogStream(aggTable)
                .print();

        eventTable.printSchema();

        env.execute();
    }
}
