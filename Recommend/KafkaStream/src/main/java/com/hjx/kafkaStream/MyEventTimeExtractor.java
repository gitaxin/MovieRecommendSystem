package com.hjx.kafkaStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Created by Axin in 2019/11/4 22:48
 */
public class MyEventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        return System.currentTimeMillis();
    }
}
