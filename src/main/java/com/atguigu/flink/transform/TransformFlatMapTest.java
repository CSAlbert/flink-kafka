package com.atguigu.flink.transform;

import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chulang
 * @date 2022/8/14 16:49
 * @description TODO
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素读取数据（比从集合中更方便，测试时更常用）
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Alice", "./home", 2000L)
        );

        // 自定义类实现
        stream.flatMap(new MyFlatMap()).print("MyFlatMap:");

        // lambda表达式实现（需要使用returns指定类型，不然会报错，不能自动确认类型）
        stream.flatMap((Event event, Collector<String> out) -> {
                    if (event.user.equals("Alice")) {
                        out.collect(event.url);
                    } else if (event.user.equals("Bob")) {
                        out.collect(event.user);
                        out.collect(event.url);
                        out.collect(event.timestamp.toString());
                    }
                }).returns(new TypeHint<String>() {
                })
                .print("lambda:");

        env.execute();


    }

    // 自定义FlatMap类
    private static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            out.collect(event.user);
            out.collect(event.url);
            out.collect(event.timestamp.toString());

        }
    }

    //

}
