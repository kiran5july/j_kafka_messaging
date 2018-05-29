package com.kiran.kafka;
/*

For KTable, must send key,value Ex:


 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
        import org.apache.kafka.common.serialization.Serdes;
        import org.apache.kafka.streams.KafkaStreams;
        import org.apache.kafka.streams.StreamsConfig;
        import org.apache.kafka.streams.kstream.KStream;
        import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KafkaStream01 {

    public static void main(String[] args) {

        Properties config = new Properties();
        //config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-starter-app");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, ""+System.currentTimeMillis());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        //Using KStream
        //KStream<String, String> kStream = builder.stream("km.words");

        //kStream.foreach((k, v) -> {
        //    System.out.println("KStream -> key="+k+"; value="+v);
        //});

        //Using KTable
        KTable<String, String> kTable = builder.table("km.words2");
        //KStream<String, String> strmkTable = kTable.toStream();

        kTable.foreach((k, v) -> {
            System.out.println("KTable/Stream -> key="+k+"; value="+v);
        });
        // do stuff
        //kStream.to("word-count-output");
        try {
            Thread.sleep(1000);
        }catch(Exception e){
            System.out.println("Exception in thread... "+e.getMessage());
        }
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();


        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
