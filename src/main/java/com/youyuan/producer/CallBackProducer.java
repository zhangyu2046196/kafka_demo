package com.youyuan.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author zhangy
 * @version 1.0
 * @description 测试kafka生产者带回调函数的发送消息
 * @date 2020/1/8 15:07
 */
public class CallBackProducer {
    //定义kafka的服务器地址
    private final static String HOST_URL = "192.168.1.18:9092,192.168.1.19:9092,192.168.1.20:9092";
    //定义kafka的发送消息的key和value序列化
    private final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //定义kafka的topic名字，需要先在服务器上创建
    private final static String TOPIC_NAME = "youyuan01";


    public static void main(String[] args) {
        //1、创建Properties配置类，并且通过ProducerConfig对象进行配置
        Properties properties = new Properties();
        //2、设置broker地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_URL);
        //3、设置发送消息的key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);

        //4、创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //5、循环发送数据
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "友缘" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("发送的broker的partition:" + recordMetadata.partition() + " offset:" + recordMetadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }

        //6、关闭资源
        producer.close();
    }

}
