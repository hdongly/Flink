package com.apple.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/28  14:32
 */
public class LoginFailDetect_Pro {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取登录事件流，提取时间戳生成水位线
        SingleOutputStreamOperator<LoginEvent> loginStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 19000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                );

        // 2. 定义模式，连续三个登录失败事件
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                }).times(3).consecutive()
                .within(Time.seconds(10));

        // 3. 将模式应用到事件流上，得到一个PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginStream.keyBy(data -> data.userId), pattern);

        // 4. 将匹配到的复杂事件拣选出来进行处理
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstFail = map.get("fail").get(0);
                LoginEvent secondFail = map.get("fail").get(1);
                LoginEvent thirdFail = map.get("fail").get(2);
                return "警告！用户 " + firstFail.userId + " 连续三次登录失败！" +
                        "登录时间：" + firstFail.ts + ", " + secondFail.ts + ", " + thirdFail.ts;
            }
        })
                .print();

        env.execute();
    }
}
