package com.atguigu.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author chulang
 * @date 2022/8/14 14:38
 * @description 循环生成数据的Source
 * 一般测试时用来模拟一个生成数据的数据源
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();    // 在指定的数据集中随机选取数据
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        // 循环生成数据
        while (running){
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔1秒生成一个点击事件，方便观测
            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
