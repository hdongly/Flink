package com.apple.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String jobName = parameterTool.get("jobName"); //提交脚本中需要显示指定--jobname dwd-LogBaseApp

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 2. 读取文件
//        DataStreamSource<String> lineStream = env.readTextFile("input/words.txt");

        // 读取文本流
        DataStreamSource<String> lineStream = env.socketTextStream("localhost", 7777);

        // 3. 分词转换并分组统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).name("").uid("").returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        // 4. 打印输出
        sum.print();

        // 5. 执行
        env.execute(jobName);
    }
}
