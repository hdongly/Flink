package com.apple.sql;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Demo004 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> clickDS = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table clickTbl = tEnv.fromDataStream(clickDS);
        Table tbl01 = tEnv.sqlQuery("select * from " + clickTbl);
        Table tbl02 = tEnv.sqlQuery("select * from " + tbl01);
        tEnv.createTemporaryView("tbl03", tbl02);
        Table tbl03 = tEnv.sqlQuery("select user, count(1) from tbl03 group by user");
        tEnv.toChangelogStream(tbl03).print();

        env.execute();
    }
}
