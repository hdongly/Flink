package com.apple.sql;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;


public class AggregateFunctionTest {
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

        // 注册自定义的聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAverage.class);

        // 调用UDF查询转换
        Table result = tableEnv.sqlQuery("select user, WeightedAvg(ts, 2) as weighted_avg " +
                "from EventTable group by user");

        // 输出到控制台
        tableEnv.executeSql("create table output (" +
                "uname STRING, " +
                "weighted_avg BIGINT)" +
                "WITH (" +
                "'connector' = 'print'" +
                ")");
        result.executeInsert("output");
    }

    // 自定义一个累加器的类型
    public static class WeightedAvgAcc {
        public Long sum = 0L;     // 加权和
        public Integer count = 0;    // 加权个数
    }

    // 自定义聚合函数，求加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAcc>{
        @Override
        public Long getValue(WeightedAvgAcc weightedAvgAcc) {
            if (weightedAvgAcc.count == 0)
                return null;
            else
                return weightedAvgAcc.sum / weightedAvgAcc.count;
        }

        @Override
        public WeightedAvgAcc createAccumulator() {
            return new WeightedAvgAcc();
        }

        public void accumulate(WeightedAvgAcc weightedAvgAcc, Long ts, Integer weight){
            weightedAvgAcc.sum += ts * weight;
            weightedAvgAcc.count += weight;
        }
    }
}
