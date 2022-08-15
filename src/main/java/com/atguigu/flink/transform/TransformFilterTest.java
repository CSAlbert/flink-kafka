package com.atguigu.flink.transform;

import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chulang
 * @date 2022/8/14 16:14
 * @description TODO
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素读取数据（比从集合中更方便，测试时更常用）
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Alice", "./home", 5000L)
        );

        // 跟map相同，也有三种方式实现（自定义类、匿名类、Lambda表达式）
        SingleOutputStreamOperator<Event> result = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.url.equals("./home");
            }
        });

        // lambda
        stream.filter(data -> data.url.equals("./home")).print("Lambda: url ");

        result.print("");

        env.execute();


    }
}
