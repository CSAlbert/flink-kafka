package com.atguigu.flink.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author: chulang
 * @DateTime: 2022/8/13 16:05
 * @Description: 数据读取操作
 **/
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1. 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        //2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(3);
        nums.add(66);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        // 从集合中读取类的集合，会对对象进行toString的输出
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",3000L));
        DataStreamSource<Event> eventStream = env.fromCollection(events);

        //3. 从元素读取数据（比从集合中更方便，测试时更常用）
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 3000L)
        );

        //4. 从socket文本流读取数据
        DataStreamSource<String> Stream4 = env.socketTextStream("linux101", 11111);

        //1-3都是有界流
//        stream1.print("1");
//        numStream.print("nums");
//        eventStream.print("event");
//        stream3.print("3");
//        Stream4.print("4");

        // 上面都是flink预实现的连接方法，还提供了通用的addSource方法
        //5. 从kafka读取数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"linux101:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("test0805", new SimpleStringSchema(), properties));

        kafkaStream.print();


        env.execute();


    }

}
