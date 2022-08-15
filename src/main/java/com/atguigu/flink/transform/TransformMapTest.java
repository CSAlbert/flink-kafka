package com.atguigu.flink.transform;

import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.swing.*;

/**
 * @author chulang
 * @date 2022/8/14 15:44
 * @description TODO
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素读取数据（比从集合中更方便，测试时更常用）
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 3000L)
        );

        //进行转换操作，提取user字段
        // 1.使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());


        // 2.使用匿名类，实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });

        // 3.传入Lambda表达式（最简洁，会有些泛型擦除问题，那时候就需要手动指定类型-returns）
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);

        result1.print();
        result2.print();
        result3.print();

        env.execute();

    }

    //自定义MapFunction
    public static class MyMapper implements MapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
