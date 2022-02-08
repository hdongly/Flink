package com.apple.chapter05;
import com.apple.bean.Event;
import com.apple.sources.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author atguigu-mqx
 */
public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));
        DataStream<Event> stream2 = env.addSource(new ClickSource());
        stream2.rebalance().print().setParallelism(4);

//        env.addSource(new ClickSource())
////                .rebalance()
//                .partitionCustom(new Partitioner<String>() {
//                    @Override
//                    public int partition(String key, int numPartitions) {
//                        return (int)(key.charAt(1) - 'a') % 4;
//                    }
//                }, new KeySelector<Event, String>() {
//                    @Override
//                    public String getKey(Event value) throws Exception {
//                        return value.user;
//                    }
//                })
//                .print();

        env.execute();
    }
}
