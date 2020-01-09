package com.youyuan.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author zhangy
 * @version 1.0
 * @description 引入自定义拦截器的生产者
 * @date 2020/1/9 9:59
 */
public class InterceptorProducer {
    //定义服务器地址
    private final static String HOST_URL = "192.168.1.18:9092,192.168.1.19:9092,192.168.1.20:9092";
    //定义topic名
    private final static String TOPIC_NAME = "youyuan01";
    //定义发送消息的key和value的序列化器
    private final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void main(String[] args) {
        //1、创建配置类
        Properties properties = new Properties();
        //2、配置服务器地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_URL);
        //3、配置消息key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        //4、配置自定义拦截器
        ArrayList<String> list = new ArrayList<>();
        list.add("com.youyuan.interceptor.TimeInterceptor");
        list.add("com.youyuan.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

        //5、创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //6、循环发送消息
        for (int i = 1; i <= 100000; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "开心工作，幸福生活"));
        }

        //7、关闭资源
        producer.close();
    }

}
