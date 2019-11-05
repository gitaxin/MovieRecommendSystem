package com.hjx.kafkaStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Created by Axin in 2019/11/4 22:24
 */
public class Application {


    public static void main(String[] args) {

        String brokers = "192.168.110.110:9092";
        String zookeeper = "192.168.110.110:2181";
        String fromTopic = "log";
        String toTopic = "recom";

        Properties settings = new Properties();

        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");//这个随称可以随便指定，相当于spark中的setAppName
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        //settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class.getName());//处理时间不同步


        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE",fromTopic)//从log这个topic中去读
                .addProcessor("PROCESS",() -> new LogProcessor(),"SOURCE")
                .addSink("SINK",toTopic,"PROCESS");//spark会从这里消费


        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();







    }
}
