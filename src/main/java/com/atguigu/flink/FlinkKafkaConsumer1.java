package com.atguigu.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author: chulang
 * @DateTime: 2022/8/8 12:23
 * @Description: TODO
 **/
public class FlinkKafkaConsumer1 {

    public static void main(String[] args) throws Exception {
        // 0 初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 kafka消费者配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "43.142.183.27:9092");

        // 2 创建kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "test0805",
                new SimpleStringSchema(),
                properties
        );

        // 3 消费者和flink流关联
        env.addSource(kafkaConsumer).print();

        // 4 执行
        env.execute();


    }

}
