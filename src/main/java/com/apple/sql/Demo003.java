package com.apple.sql;

import com.apple.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo003 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String sql = "CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMALTranf(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")";
        tEnv.executeSql(sql);
        Table resultTbl = tEnv.sqlQuery("select order_time from Orders");
        tEnv.toDataStream(resultTbl).print();
        env.execute();
    }
}
