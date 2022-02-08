package com.apple.sql;

import com.apple.bean.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableToStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                Event.of("Alice", "./home", 1000L),
                Event.of("Bob", "./cart", 1000L),
                Event.of("Alice", "./prod?id=1", 5 * 1000L),
                Event.of("Cary", "./home", 60 * 1000L),
                Event.of("Bob", "./prod?id=3", 90 * 1000L),
                Event.of("Alice", "./prod?id=7", 105 * 1000L));
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);
        tblEnv.createTemporaryView("StuInfo", eventStream);
        Table aliceVisitTable = tblEnv.sqlQuery("select user, url from StuInfo where user = 'Alice'");
        Table urlCountTable = tblEnv.sqlQuery("SELECT user, COUNT(url) FROM StuInfo GROUP BY user");
        tblEnv.toDataStream(aliceVisitTable).print("Alice ");
        env.execute();
    }
}
