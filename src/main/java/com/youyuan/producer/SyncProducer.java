package com.youyuan.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author zhangy
 * @version 1.0
 * @description  同步发送案例
 *
 * kafka默认是异步发送
 *
 * @date 2020/1/8 16:23
 */
public class SyncProducer {
    //定义服务器地址
    private final static String HOST_URL = "192.168.1.18:9092,192.168.1.19:9092,192.168.1.20:9092";
    //定义topic名
    private final static String TOPIC_NAME = "youyuan01";
    //生产者发送消息key和value的序列化器
    private final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1、定义配置类
        Properties properties = new Properties();
        //2、配置broker地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_URL);
        //3、配置生产者发送消息的key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);

        //4、创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //5、发送数据  带回调函数
        for (int i = 1; i <= 10; i++) {
            Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "中国--" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        System.out.println("生产发送消息返回数据partition:" + recordMetadata.partition() + " offset:" + recordMetadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });

            send.get();//通过future对象的get方法实现同步发送
        }

        //7、关闭资源
        producer.close();
    }
}
