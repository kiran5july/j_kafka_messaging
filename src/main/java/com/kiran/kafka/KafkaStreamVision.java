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

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamVision {

    private final static String TOPIC_STATS = //"km.vision.endpointstats.topic.test2";
                                                "rtalab.allstate.is.vision.stats";
    private final static String TOPIC_EVENTS = "rtalab.allstate.is.vision.test";

    private final static String TOPIC_RESULT = //"km.wordcounts";
                                            "rtalab.allstate.is.vision.results_str_spark";

    private final static String BOOTSTRAP_SERVERS =     //"localhost:9092";
            "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092, lxe0964.allstate.com:9092, lxe0965.allstate.com:9092";
    private final static String GROUP_ID = "KM_Consumer_Group3";
    //private logger = Logger.getLogger(this.getClass.getName)
    private final static String SCHEMA_REG_URL = "http://lxe0961.allstate.com:8081";

    public static void main(String[] args) {

        //Get Stats
        Properties configStats = new Properties();
        configStats.put(StreamsConfig.APPLICATION_ID_CONFIG, ""+System.currentTimeMillis()); //dynamic application id to get all contents every time
        configStats.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configStats.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest, earliest, none
        //configStats.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //Error
        configStats.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //configStats.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configStats.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

        KStreamBuilder builder2 = new KStreamBuilder();
        KTable<String, String> tblKeys = builder2.table(TOPIC_STATS);
        //GlobalKTable<String, String> tblKeys = builder2.globalTable("km.words.keys");
        tblKeys.foreach((k, v) -> {
            System.out.println("KTable -> key="+k+"; value="+v);
        });

        Properties configEvts = new Properties();
        //configEvts.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-starter-app");
        configEvts.put(StreamsConfig.APPLICATION_ID_CONFIG, "APPL_KSTREAM_002");
        configEvts.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configEvts.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //latest, earliest, none
        //configEvts.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        configEvts.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //configEvts.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configEvts.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class); //SpecificAvroSerde

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> strmWords = builder.stream(TOPIC_EVENTS);

        strmWords.foreach((k, v) -> {
            System.out.println("KStream -> key="+k+"; value="+v);
        });
/*
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
*/
        KafkaStreams streams = new KafkaStreams(builder, configEvts);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        KafkaStreams strmTable = new KafkaStreams(builder2, configStats);
        strmTable.cleanUp();
        strmTable.start();

        // print the topology
        //System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(strmTable::close));

    }

}
