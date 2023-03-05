package com.apple.webui;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class LocalUI {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        // 2. 滚动窗口聚合
        Table windowAggTable = tableEnv.sqlQuery("select user, count(url) as cnt, " +
                " window_start, window_end " +
                "from TABLE(" +
                "    TUMBLE(TABLE EventTable, DESCRIPTOR(rt), INTERVAL '10' SECOND)" +
                ")" +
                "group by user, " +
                "  window_start, " +
                "  window_end");
        tableEnv.createTemporaryView("AggTable", windowAggTable);

        // 3. 基于窗口聚合后的结果表，用Over窗口聚合统计每行数据的按照cnt排序的行号
        Table result = tableEnv.sqlQuery("select * from (" +
                "  select *, ROW_NUMBER() OVER (" +
                "         PARTITION BY window_start, window_end " +
                "         ORDER BY cnt DESC " +
                "     ) as row_num " +
                "  FROM AggTable" +
                ") WHERE row_num <= 2");

        // 输出到控制台
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "cnt BIGINT," +
                "window_start TIMESTAMP, " +
                "window_end TIMESTAMP," +
                "row_num BIGINT )" +
                "WITH (" +
                "'connector' = 'print'" +
                ")");
        result.executeInsert("output");

    }
}

