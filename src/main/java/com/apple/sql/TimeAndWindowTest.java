package com.apple.sql;

import com.apple.sources.ClickSource;
import com.apple.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.读取数据源
        // 1.1 创建连接器表，读取文件数据
        tableEnv.executeSql("create table ClickTable (" +
                "`user` STRING, " +
                "url STRING, " +
                "ts BIGINT, " +
                "rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +    // 额外定义的时间属性字段
                "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND ) " +
                "WITH (" +
                "'connector' = 'filesystem', " +
                "'path' = 'input/clicks.txt', " +
                "'format' = 'csv')");

        // 1.2 流转成表的过程中定义时间属性字段
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 需要明确指定时间字段
        Table eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("rt").rowtime());
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 2. 查询转换-聚合
        // 2.1 一般分组聚合
        Table groupAggTable = tableEnv.sqlQuery("select user, count(1) from ClickTable group by user");

        // 2.2 窗口聚合
        // 2.2.1 老版本分组窗口
        Table groupTumbleWindowAggTable = tableEnv.sqlQuery("select user, count(url) as cnt, " +
                "    TUMBLE_END(rt, INTERVAL '10' SECOND) AS win_end " +
                "from EventTable " +
                "group by " +
                "    user, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        // 2.2.2 新版本窗口TVF
        Table tumbleWindowAggTable = tableEnv.sqlQuery("select user, count(url) as cnt, " +
                "  window_end as wend " +
                "from TABLE(" +
                "  TUMBLE(TABLE EventTable, DESCRIPTOR(rt), INTERVAL '10' SECOND)" +
                ")" +
                "group by user," +
                "  window_start," +
                "  window_end");

        // 滑动窗口
        Table hopWindowAggTable = tableEnv.sqlQuery("select user, count(url) as cnt, " +
                "  window_end as wend " +
                "from TABLE(" +
                "  HOP(TABLE EventTable, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ")" +
                "group by user," +
                "  window_start," +
                "  window_end");

        // 累积窗口
        Table cumulateWindowAggTable = tableEnv.sqlQuery("select user, count(url) as cnt, " +
                "  window_end as wend " +
                "from TABLE(" +
                "  CUMULATE(TABLE EventTable, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ")" +
                "group by user," +
                "  window_start," +
                "  window_end");

        // 3. 打印输出
        // 3.1 转换成流打印输出
//        tableEnv.toChangelogStream(groupAggTable)
//                .print("group-agg");

        // 3.2 创建连接器表
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "cnt BIGINT," +
                "win_end TIMESTAMP )" +
                "WITH (" +
                "'connector' = 'print'" +
                ")");
        cumulateWindowAggTable.executeInsert("output");

//        env.execute();
    }
}
