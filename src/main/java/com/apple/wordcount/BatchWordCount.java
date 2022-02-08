package com.apple.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author atguigu-mqx
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception{
        // 1. 创建一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        DataSource<String> lineDS = env.readTextFile("input/words.txt");

        // 3. 转换数据格式，拆分单词并转换成(word, 1)二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne.groupBy(0);

        // 5. 组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        sum.print();
    }
}
