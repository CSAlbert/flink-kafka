package com.atguigu.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chulang
 * @date 2022/8/14 14:35
 * @description 使用自定义数据源
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> customSource = env.addSource(new ClickSource());

        customSource.print();

        env.execute();

    }
;
}
