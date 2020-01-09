package com.youyuan.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zhangy
 * @version 1.0
 * @description 自定义拦截器用于在消息发送前在消息内容前加上时间戳
 * @date 2020/1/9 9:52
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    /**
     * 发送消息前处理
     *
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //获取消息内容
        String value = producerRecord.value();
        //在消息内容前增加时间戳
        value = System.currentTimeMillis() + ":" + value;
        //创建一个新的ProducerRecord
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.key(), value);
    }

    /**
     * 发送消息后回调时处理
     *
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    /**
     * 关闭资源时处理
     */
    @Override
    public void close() {

    }

    /**
     * 获取配置信息
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
