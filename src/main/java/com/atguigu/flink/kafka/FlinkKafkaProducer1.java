package com.atguigu.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author: chulang
 * @DateTime: 2022/8/8 11:46
 * @Description: TODO
 **/
public class FlinkKafkaProducer1 {

    public static void main(String[] args) throws Exception {

        // 0 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1 读取集合中数据
        ArrayList<String> wordsList = new ArrayList<>();
        wordsList.add("Hello");
        wordsList.add("Flink kafka");
        DataStreamSource<String> stream = env.fromCollection(wordsList);

        // 2 kafka生产者配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"43.142.183.27:9092");

        // 3 创建kafka生产者
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "test0805",
                new SimpleStringSchema(),
                properties
        );

        // 4 生产者和flink流关联
        stream.addSink(kafkaProducer);

        // 5 执行
        env.execute();
    }
}
