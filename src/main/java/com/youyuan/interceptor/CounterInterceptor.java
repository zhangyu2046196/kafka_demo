package com.youyuan.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zhangy
 * @version 1.0
 * @description 自定义拦截器，用于统计生产者发送消息后发送成功和失败数量
 * @date 2020/1/9 9:56
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    //定义成功数量
    private int success;
    //定义失败数量
    private int errors;

    /**
     * 发送消息前处理
     *
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    /**
     * 发送消息后接收回调时处理
     *
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (recordMetadata != null) {
            success++;
        } else {
            errors++;
        }
    }

    /**
     * 关闭资源时处理
     */
    @Override
    public void close() {
        System.out.println("success:" + success);
        System.out.println("error:" + errors);
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
