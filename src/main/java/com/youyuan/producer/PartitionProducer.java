package com.youyuan.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author zhangy
 * @version 1.0
 * @description 自定义分区器的生产者
 * @date 2020/1/8 15:56
 */
public class PartitionProducer {
    //定义服务器地址
    private final static String HOST_URL = "192.168.1.18:9092,192.168.1.19:9092,192.168.1.20:9092";
    //定义topic名
    private final static String TOPIC_NAME = "youyuan01";
    //生产者发送消息key和value的序列化器
    private final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //自定义分区器的全路径名
    private final static String MY_PARTITION = "com.youyuan.partition.MyPartition";

    public static void main(String[] args) {
        //1、定义配置类
        Properties properties = new Properties();
        //2、配置broker地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_URL);
        //3、配置生产者发送消息的key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        //4、自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MY_PARTITION);

        //5、创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //6、发送数据  带回调函数
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "中国--" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        System.out.println("生产发送消息返回数据partition:" + recordMetadata.partition() + " offset:" + recordMetadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }

        //7、关闭资源
        producer.close();
    }

}
