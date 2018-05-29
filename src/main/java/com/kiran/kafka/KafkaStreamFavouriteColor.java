package com.kiran.kafka;


/*  Steps
*From Kafka course by Stephane Maarek
*
cd C:\km_apps\kafka_2.11-1.0.1

#Start zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

#start Kafka broker
.\bin\windows\kafka-server-start.bat .\config\server.properties

#Create topics
.\bin\windows\kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor=1 --partitions=2 --topic favourite-colour-input
#Create intermediary topic
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor=1 --topic user-keys-and-colors
--config cleanup.policy=compact
#Create output log compacted topic
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor=1 --topic favourite-colour-output
--config cleanup.policy=compact


#build & run jar
#Add plugins for Java 8 & fat jar in pom.xml
cd km/km_proj/j_kafka_messaging/
mvn clean package
[INFO] Building jar: /home/kiran/km/km_proj/j_kafka_messaging/target/j_kafka_messaging-1.0-SNAPSHOT-jar-with-dependencies.jar

java -jar /home/kiran/km/km_proj/j_kafka_messaging/target/j_kafka_messaging-1.0-SNAPSHOT-jar-with-dependencies.jar

#start consumer
.\bin\windows\kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic favourite-colour-output --from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


#start producer & enter few words
.\bin\windows\kafka-console-producer.sh --broker-list localhost:9092 --topic favourite-colour-input \
--property "parse.key=true" \
  --property "key.separator=,"
stephane,blue
alice,red

*/

import java.util.Properties;
        import java.util.Arrays;
        import java.util.stream.Collectors;

        import org.apache.kafka.clients.consumer.ConsumerConfig;
        import org.apache.kafka.common.serialization.Serdes;
        import org.apache.kafka.streams.KafkaStreams;
        import org.apache.kafka.streams.KeyValue;
        import org.apache.kafka.streams.StreamsConfig;
        import org.apache.kafka.streams.kstream.KStream;
        import org.apache.kafka.streams.kstream.KStreamBuilder;
        import org.apache.kafka.streams.kstream.KTable;

public class KafkaStreamFavouriteColor {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "km_favourite-colour-java");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KStreamBuilder builder = new KStreamBuilder();

        // Step 1: We create the topic of users keys to colours
        KStream<String, String> textLines = builder.stream("favourite-colour-input");

        KStream<String, String> usersAndColours = textLines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key, value) -> value.contains(","))
                // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // 3 - we get the colour from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 4 - we filter undesired colours (could be a data sanitization step
                .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

        usersAndColours.to("user-keys-and-colours");

        // step 2 - we read that topic as a KTable so that updates are read correctly
        KTable<String, String> usersAndColoursTable = builder.table("user-keys-and-colours");

        // step 3 - we count the occurences of colours
        KTable<String, Long> favouriteColours = usersAndColoursTable
                // 5 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count("CountsByColours");

        // 6 - we output the results to a Kafka Topic - don't forget the serializers
        favouriteColours.to(Serdes.String(), Serdes.Long(),"favourite-colour-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
