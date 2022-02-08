package com.apple.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/28  11:07
 */
public class LoginFailDetectExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取登录事件流，提取时间戳生成水位线
        SingleOutputStreamOperator<LoginEvent> loginStream = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new LoginEvent(fields[0].trim(), fields[1].trim(), fields[2].trim(), Long.valueOf(fields[3].trim()));
                    }
                })
//        SingleOutputStreamOperator<LoginEvent> loginStream = env.fromElements(
//                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
//                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
//                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
//                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
//                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
//                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
//                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
//                new LoginEvent("user_2", "192.168.1.29", "fail", 19000L)
//        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                );

        // 2. 定义模式，连续三个登录失败事件
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                }).within(Time.seconds(10));

        // 3. 将模式应用到事件流上，得到一个PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginStream.keyBy(data -> data.userId), pattern);

        // 定义一个侧输出流标签，用来存放迟到数据
        OutputTag<LoginEvent> lateDataTag = new OutputTag<LoginEvent>("late") {
        };

        // 4. 将匹配到的复杂事件拣选出来进行处理
        SingleOutputStreamOperator<String> result = patternStream
                .sideOutputLateData(lateDataTag)
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent firstFail = map.get("first").get(0);
                        LoginEvent secondFail = map.get("second").get(0);
                        LoginEvent thirdFail = map.get("third").get(0);
                        return "警告！用户 " + firstFail.userId + " 连续三次登录失败！" +
                                "登录时间：" + firstFail.ts + ", " + secondFail.ts + ", " + thirdFail.ts;
                    }
                });
        result.print("result");
        result.getSideOutput(lateDataTag).print("late");

        env.execute();
    }
}
