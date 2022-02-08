package com.apple.chapter05;

import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author atguigu-mqx
 */
public class SinkToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L),
//                new Event("Alice", "./prod?id=100", 3000L),
//                new Event("Alice", "./prod?id=200", 3500L),
//                new Event("Bob", "./prod?id=2", 2500L),
//                new Event("Alice", "./prod?id=300", 3600L),
//                new Event("Bob", "./home", 3000L),
//                new Event("Bob", "./prod?id=1", 2300L),
//                new Event("Bob", "./prod?id=3", 3300L));
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 创建一个到redis连接的配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .build();

        stream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    // 实现RedisMapper接口
    public static class MyRedisMapper implements RedisMapper<Event>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event data) {
            return data.user;
        }

        @Override
        public String getValueFromData(Event data) {
            return data.url;
        }
    }
}
