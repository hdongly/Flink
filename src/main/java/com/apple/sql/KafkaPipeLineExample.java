package com.apple.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/28  10:01
 */
public class KafkaPipeLineExample {
    public static void main(String[] args) throws Exception{
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建到kafka的连接器表
        tableEnv.executeSql("CREATE TABLE ClickTable (" +
                "user_name STRING, " +
                "url STRING, " +
                "ts BIGINT )" +
                "WITH (" +
                "'connector' = 'kafka', " +
                "'topic' = 'clicks', " +
                "'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "'format' = 'csv'" +
                ")");
        // 打印输出
//        tableEnv.executeSql("create table output (" +
//                "uname STRING, " +
//                "cnt BIGINT)" +
//                "WITH (" +
//                "'connector' = 'print'" +
//                ")");
        // 写入文件，只能是追加查询结果
//        tableEnv.executeSql("create table output (" +
//                "uname STRING, " +
//                "url STRING)" +
//                "WITH (" +
//                "'connector' = 'filesystem', " +
//                "'path' = 'output', " +
//                "'format' = 'csv')");

        tableEnv.executeSql("CREATE TABLE output (" +
                "user_name STRING, " +
                "cnt BIGINT," +
                "PRIMARY KEY (user_name) NOT ENFORCED )" +
                "WITH (" +
                "'connector' = 'upsert-kafka', " +
                "'topic' = 'pv-by-user', " +
                "'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "'key.format' = 'csv', " +
                "'value.format' = 'csv'" +
                ")");

        Table result = tableEnv.sqlQuery("select user_name, count(url) as cnt from ClickTable group by user_name");
        Table result2 = tableEnv.sqlQuery("select user_name, url from ClickTable");

        result.executeInsert("output");
    }
}
