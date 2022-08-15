package com.atguigu.flink.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @Author: chulang
 * @DateTime: 2022/8/8 13:47
 * @Description: TODO
 **/
public class test01 {
    public static void main(String[] args) throws Exception {
        // 1.初始化flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2.创建source
        // 基于File的数据源 readTextFile(path)
//        DataStreamSource<String> streamSource = env.readTextFile("D:\\FlinkProjects\\flink-kafka\\src\\main\\resources\\input\\test.txt");

        // 基于Socket的数据源（nc -lk 11111）
        DataStreamSource<String> streamSource = env.socketTextStream("43.142.183.27", 11111);

        // 基于集合（Collection）的数据源
//        List<Person> people = new ArrayList<Person>();
//        people.add(new Person("张三", 21));
//        people.add(new Person("李四", 16));
//        people.add(new Person("王老五", 35));
//
//        DataStreamSource<Person> collectionSource = env.fromCollection(people);


        // 3.指定转换操作Transformation

        // 筛选年龄大于18的用户(基于集合的数据源)
//        SingleOutputStreamOperator<Person> adults = collectionSource.filter(new FilterFunction<Person>() {
//            @Override
//            public boolean filter(Person person) throws Exception {
//                return person.age >= 18;
//            }
//        });
//        adults.print();

        // 转换基本操作
        // Map方法:DataStream → DataStream
//        SingleOutputStreamOperator<String> streamMap = streamSource.map(x -> x + "+map后结果");
//        streamMap.print();


        // FlatMap方法：DataStream → DataStream
//        SingleOutputStreamOperator<Object> streamFlatMap = streamSource.flatMap(new FlatMapFunction<String, Object>() {
//            @Override
//            public void flatMap(String s, Collector<Object> collector) throws Exception {
//                //将一行字符串按-切分成一个字符串数组
//                String[] words = s.split("-");
//                for (String word : words) {
//                    // 输出
//                    collector.collect(word);
//                }
//            }
//        });
//        streamFlatMap.print();

        // Filter方法：DataStream → DataStream
//        SingleOutputStreamOperator<String> streamFilter = streamSource.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return s.equals("dddd");
//            }
//        });
//        streamFilter.print();

        // KeyBY：DataStream → KeyedStream
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(word);

                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

//        keyedStream.print();

        // Reduce :KeyedStream → DataStream
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
            }
        });

        reduce.print();

        // Window :KeyedStream → WindowedStream
//        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5)));



        // 4.指定结果数据存放位置Sink

        // writeAsText、writeAsCsv、print/printToErr、writeUsingOutputFormat、writeToSocket
        // 输出流内数据
//        streamSource.print();

        // 5.触发程序执行
        env.execute("FirstJob");

    }

    // POJO类（基于集合的数据源使用）
//    public static class Person {
//        public String name;
//        public Integer age;
//
//        public Person() {};
//
//        public Person(String name, Integer age) {
//            this.name = name;
//            this.age = age;
//        };
//
//        public String toString() {
//            return this.name.toString() + ": age " + this.age.toString();
//        };
//    }

}
