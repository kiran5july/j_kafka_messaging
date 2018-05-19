package com.kiran.kafka;

/*  Steps

cd C:\km_apps\kafka_2.11-1.0.1

#Start zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

#start Kafka broker
.\bin\windows\kafka-server-start.bat .\config\server.properties

.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor=1 --topic km.wordcounts --partitions 4
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor=1 --topic km.words --partitions 4

#start consumer
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic km.wordcounts --from-beginning

#start producer
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic km.words

*/

import com.allstate.bigdatacoe.vision.events.SpanEvent;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
        import org.apache.kafka.streams.KafkaStreams;
        import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
        import java.util.Properties;

public class KafkaStreamWordCounts {

    private final static String TOPIC = //"km.vision.endpointstats.topic.test2";
                                        //"rtalab.allstate.is.vision.test";
                                        "km.words";

    private final static String TOPIC_RESULT = "km.wordcounts";

    private final static String BOOTSTRAP_SERVERS =     "localhost:9092";
            //"lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092, lxe0964.allstate.com:9092, lxe0965.allstate.com:9092";
    //private final static String SCHEMA_REG_URL = "http://lxe0961.allstate.com:8081";

    public static void main(final String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "km_kafka_stream_prg");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //config.put("schema.registry.url", SCHEMA_REG_URL);
        //config.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountryGroup1");

        KStreamBuilder builder = new KStreamBuilder();
        //System.out.println("KTable data:" + tblData);

        KStream<String, String> textLines = builder.stream(TOPIC);

        KTable<String, Long> wordCounts = textLines
                .mapValues(l -> l.toLowerCase())
                .flatMapValues(x -> Arrays.asList(x.split(" ")))
                .selectKey((k,v) -> v)
                .groupByKey()
                .count("wordcounts");


        wordCounts.to(Serdes.String(), Serdes.Long(), TOPIC_RESULT);

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}