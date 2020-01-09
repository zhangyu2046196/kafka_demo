package com.youyuan.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author zhangy
 * @version 1.0
 * @description  自定义分区器
 * @date 2020/1/8 15:54
 */
public class MyPartition implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //根据自身业务逻辑来自定义分区规则
//        Integer integer = cluster.partitionCountForTopic(s);//获取topic的分区数
//        return o.toString().hashCode()%integer;//
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
