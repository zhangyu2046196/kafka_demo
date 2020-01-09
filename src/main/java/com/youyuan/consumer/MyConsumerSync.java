package com.youyuan.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zhangy
 * @version 1.0
 * @description kafka消费者  同步提交offset
 * @date 2020/1/8 18:15
 */
public class MyConsumerSync {
    //定义服务器地址
    private final static String HOST_URL = "192.168.1.18:9092,192.168.1.19:9092,192.168.1.20:9092";
    //定义消费组名
    private final static String GROUP_ID = "bigdata";
    //定义消息key和value的解码器
    private final static String DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) throws IOException {
        //1、创建配置类
        Properties properties = new Properties();
        //2、配置服务器地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_URL);
        //3、配置消息的key和value的解码器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DESERIALIZER);
        //4、配置消费组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        //5、打开消费者自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //6、配置消费者提交offset的时间1秒钟提交一次
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//重置offset，只有当修改消费者组或者是一个新建的消费者组才加上重置offset后才会重新消费topic中的数据

        //7、定义消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //8、设置订阅的topic 如果zk上topic不存在也可以,可以订阅多个
        consumer.subscribe(Arrays.asList("youyuan01", "youyuan02"));
        //9、接收消息
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);//接收消息，100毫秒
            for (ConsumerRecord consumerRecord : consumerRecords) {
                System.out.println("消费者消费消息,key:" + consumerRecord.key() + " value:" + consumerRecord.value());
            }

            consumer.commitAsync();//同步提交offset会造成线程阻塞，提交失败会重试，问题为降低系统吞吐量
        }

        //9、关闭资源
        //consumer.close();
    }

}
