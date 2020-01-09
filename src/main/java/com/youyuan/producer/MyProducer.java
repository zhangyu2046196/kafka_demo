package com.youyuan.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author zhangy
 * @version 1.0
 * @description kafka生产者实例代码
 * @date 2020/1/8 11:39
 */
public class MyProducer {

    //定义kafka集群地址
    private final static String HOST_URL = "192.168.1.18:9092,192.168.1.19:9092,192.168.1.20:9092";
    //定义kafka发送消息的topic
    private final static String TOPIC_NAME = "youyuan_it";
    //定义ack应答机制
    private final static String ACK = "-1";
    //定义send线程批量发送消息的大小 默认是16k
    private final static int BATCH_SIZE = 16384;
    //定义main线程和send线程的共享变量RecordAccumulator的缓存大小  默认是32m
    private final static long BUFFER_MEMORY_CONFIG = 33554432;
    //定义send线程批量发送数据的等待时间  默认1毫秒
    private final static int LINGER_MS_CONFIG = 1;
    //定义发送数据的key和value的序列化器
    private final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void main(String[] args) {

        //1、创建kafka配置类
        Properties properties = new Properties();
        //2、配置kafka集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_URL);
        //3、配置kafka的ack应答机制
        properties.put(ProducerConfig.ACKS_CONFIG, ACK);
        //4、配置kafka的send线程批量发送消息的大小 默认16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        //5、配置kafka的send线程发送消息等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG);
        //6、配置kafka的main线程和send线程操作共享缓冲区的大小  默认32m
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY_CONFIG);
        //7、配置kafka的发送数据的key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        //8、创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //9、生产者发送消息到broker
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "友缘网------" + i));
        }

        //10、关闭资源
        producer.close();
    }

}
