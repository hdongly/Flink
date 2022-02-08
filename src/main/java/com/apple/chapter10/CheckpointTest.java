package com.apple.chapter10;

import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/25  15:09
 */
public class CheckpointTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        // 检查点存储路径
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://namenode:40010/flink/checkpoints"));

        // 检查点高级配置
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);     // 模式
        checkpointConfig.setCheckpointTimeout(5 * 10 * 1000L);    // 超时时间
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(1);
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setAlignmentTimeout(Duration.ofSeconds(2));
    }
}
