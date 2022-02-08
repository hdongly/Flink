package com.apple.sql;

import com.apple.sources.ClickSource;
import com.apple.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/27  16:34
 */
public class ScalarFunctionTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 1. 读取数据源，转换成表
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
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

        // 注册自定义的标量函数
        tableEnv.createTemporarySystemFunction("MyHash", MyHash.class);

        // 调用UDF查询转换
        Table result = tableEnv.sqlQuery("select user, MyHash(url) from EventTable");

        // 输出到控制台
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "myhash INT)" +
                "WITH (" +
                "'connector' = 'print'" +
                ")");
        result.executeInsert("output");
    }

    // 自定义标量函数，实现求一个String的哈希值
    public static class MyHash extends ScalarFunction{
        // 实现计算方法
        public int eval(String str){
            return str.hashCode();
        }
    }
}
