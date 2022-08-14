package com.atguigu.flink.chapter05;

import java.sql.Timestamp;

/**
 * @Author: chulang
 * @DateTime: 2022/8/13 15:58
 * @Description: 事件类（用户访问事件，三个字段：用户、URL、时间）
 *
 * 这里需要注意，我们定义的 Event，有这样几个特点：
 * ⚫ 类是公有（public）的
 * ⚫ 有一个无参的构造方法
 * ⚫ 所有属性都是公有（public）的
 * ⚫ 所有属性的类型都是可以序列化的
 *
 * Flink 会把这样的类作为一种特殊的 POJO 数据类型来对待，方便数据的解析和序列化。
 * 另外我们在类中还重写了 toString 方法，主要是为了测试输出显示更清晰。关于 Flink 支持的
 * 数据类型，我们会在后面章节做详细说明。
 * 我们这里自定义的 Event POJO 类会在后面的代码中频繁使用，所以在后面的代码中碰到
 * Event，把这里的 POJO 类导入就好了。
 * 注：Java 编程比较好的实践是重写每一个类的 toString 方法，来自 Joshua Bloch 编写的
 * 《Effective Java》。
 **/
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    // 必须有的空参构造方法
    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
