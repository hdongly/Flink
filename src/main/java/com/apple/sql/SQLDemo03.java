package com.apple.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class SQLDemo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);
//        tblEnv.executeSql("create table clicks (" +
//                "`user` string, " +
//                "`url` string, " +
//                "`ts` BIGINT) " +
//                " WITH (" +
//                " 'connector' = 'filesystem', " +
//                " 'path' = 'input/clicks.txt', " +
//                " 'format' = 'csv'" +
//                ")");
        tblEnv.executeSql("CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")");
        Table clicksTable = tblEnv.sqlQuery("select price, buyer, order_time from Orders");
//        Table table2 = tblEnv.from("Orders").select($("price"), $("buyer"), $("order_time"));
        tblEnv.createTemporaryView("new_orders", clicksTable);
        Table table2 = tblEnv.from("new_orders").select($("price"), $("buyer"), $("order_time"));
//        tblEnv.toDataStream(table2).print();
        tblEnv.toChangelogStream(table2).print();
        env.execute();
    }
}
