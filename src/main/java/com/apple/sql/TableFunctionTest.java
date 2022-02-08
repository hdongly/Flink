package com.apple.sql;

import com.apple.sources.ClickSource;
import com.apple.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TableFunctionTest {
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

        // 注册自定义的表函数
        tableEnv.createTemporarySystemFunction("MySplit", MySplit.class);

        // 调用UDF查询转换
        Table result = tableEnv.sqlQuery("select user, url, word, length " +
                "from EventTable, LATERAL TABLE( MySplit(url) ) AS T(word, length)");

        // 输出到控制台
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "url STRING, " +
                "word STRING, " +
                "length INT)" +
                "WITH (" +
                "'connector' = 'print'" +
                ")");
        result.executeInsert("output");
    }

    // 自定义表函数
    public static class MySplit extends TableFunction<Tuple2<String, Integer>> {
        // 实现计算方法
        public void eval(String str){
            String[] fields = str.split("\\?");
            for (String field : fields){
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}
