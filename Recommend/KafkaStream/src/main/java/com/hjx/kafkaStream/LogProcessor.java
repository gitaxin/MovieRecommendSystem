package com.hjx.kafkaStream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Created by Axin in 2019/11/4 22:32
 *
 * 处理数据类
 */


/**
 * flume配置文件
 * a2.sources = exectail
 * a2.sinks = memoryChannel
 * a2.channels = kafkasink
 * # Describe/configure the source
 * a2.sources.exectail.type = exec
 * a2.sources.exectail.command = tail -f /opt/module/tomcat-8.5.23/logs/catalina.out
 * a2.sources.exectail.channels = memoryChannel
 *
 * # Describe the sink
 * a2.sinks.kafkasink.type = org.apache.flume.sink.kafka.KafkaSink
 * a2.sinks.kafkasink.kafka.topic = log
 *
 * a2.sinks.kafkasink.kafka.bootstrap.servers = 192.168.110.110:9092
 * a2.sinks.kafkasink.kafka.producer.acks = 1
 * a2.sinks.kafkasink.kafka.flumeBatchSize = 20
 * a2.sinks.kafkasink.channel = memoryChannel
 *
 * # Use a channel which buffers events in memory
 * a2.channels.memoryChannel.type = memory
 * a2.channels.memoryChannel.capacity = 10000
 *
 */
public class LogProcessor implements Processor<byte[],byte[]> {

    private ProcessorContext context;//上下文对象，类似于SparkContext

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = context;

    }

    /**
     *
     * @param bytes
     * @param line  输入数据
     */
    @Override
    public void process(byte[] bytes, byte[] line) {

        //数据处理逻辑
        String input = new String(line);

        /**
         *flume->kafkastream->spark->mongodb
         * MOVIE_RATING_PREFIX:1|2|5.0|1564412038
         * 说明：用户1对电影2打了5.0分，在1564412038这个时间
         */
        if (input.contains("MOVIE_RATING_PREFIX:")) {
            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor",input.getBytes());//logProcessor相当于标识

        }



    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
