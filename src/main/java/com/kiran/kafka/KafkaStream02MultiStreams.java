package com.kiran.kafka;

/*
Finds the words count of specific words from the key topic.

#Start producer for words keys in format (key,value)
kafka-console-producer.sh --broker-list localhost:9092 --property "parse.key=true"   --property "key.separator=," --topic km.words.keys
kafka,kafka
apache,apache

#Start the program

#Start the producer for actual words



 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class KafkaStream02MultiStreams {

    private final static String TOPIC_RESULT = "km.wordcounts";

    public static void main(String[] args) {

        //Get all keys before actual words
        Properties config2 = new Properties();
        config2.put(StreamsConfig.APPLICATION_ID_CONFIG, ""+System.currentTimeMillis()); //dynamic to get all contents everytime
        config2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest, earliest, none
        //config2.put(StreamsConfig.consumerPrefix(AUTO_OFFSET_RESET_CONFIG), "earliest"); //NOT WORKING
        //config2.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //Error
        config2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder2 = new KStreamBuilder();
        KTable<String, String> tblKeys = builder2.table("km.words.keys");
        //GlobalKTable<String, String> tblKeys = builder2.globalTable("km.words.keys");
        tblKeys.foreach((k, v) -> {
            System.out.println("KTable -> key="+k+"; value="+v);
        });

        Properties config1 = new Properties();
        //config1.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-starter-app");
        config1.put(StreamsConfig.APPLICATION_ID_CONFIG, "APPL_KSTREAM_002");
        config1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config1.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest, earliest, none
        //config1.put(StreamsConfig.consumerPrefix(AUTO_OFFSET_RESET_CONFIG), "earliest"); //NOT WORKING
        //config1.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> strmWords = builder.stream("km.words");

        strmWords.foreach((k, v) -> {
            System.out.println("KStream -> key="+k+"; value="+v);
        });

        KTable<String, Long> wordsKeysCounts = strmWords
                .mapValues(l -> l.toLowerCase())
                //.flatMap((x,y) -> Arrays.asList(y.split(" ")));
                .flatMapValues(x -> Arrays.asList(x.split(" ")))
                .selectKey((k,v) -> v)
                .join(tblKeys,
                        (k,v) -> k
                )
                .groupByKey()
                .count("wordcounts");

        wordsKeysCounts.to(Serdes.String(), Serdes.Long(), TOPIC_RESULT);

        wordsKeysCounts.foreach((k,v) ->{
            System.out.println("Resule KTable -> key="+k+"; value="+v);
        });

        KafkaStreams streams = new KafkaStreams(builder, config1);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        KafkaStreams strmTable = new KafkaStreams(builder2, config2);
        strmTable.cleanUp();
        strmTable.start();

        // print the topology
        //System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(strmTable::close));

    }

}
