package com.apple.sources;

import com.apple.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author atguigu-mqx
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个标志位，用来控制数据的生成
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 随机生成数据
        Random random = new Random();

        // 定义数据选取的范围
        String[] users = {"Alice", "Bob", "Cary", "Mary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=100", "./prod?id=200"};

        // 循环生成数据
        while(running){
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
