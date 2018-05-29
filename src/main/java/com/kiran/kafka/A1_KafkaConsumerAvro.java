package com.kiran.kafka;

/*
 *
 *
 */
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;

/**
 * Add the following VM options to run this class
 * <p>
 * -Djava.security.auth.login.config=/Users/dvege/IdeaProjects/local/VisionAnomalyDetection/src/test/resources/rtalab_vision_jaas.conf
 * -Djava.security.krb5.conf=/Users/dvege/IdeaProjects/local/VisionAnomalyDetection/src/test/resources/krb5.conf
 * -Djavax.security.auth.useSubjectCredsOnly=false
 */

public class A1_KafkaConsumerAvro {

    // Variables
    String factSpanTopic = "rtalab.allstate.is.vision.test";
    String statsTopic = "rtalab.allstate.is.vision.stats";
    String alertTopic = "rtalab.allstate.is.vision.results_spark";
    String bootstrapServers = "lxe0961.allstate.com:9092,lxe0962.allstate.com:9092,lxe0963.allstate.com:9092,lxe0964.allstate.com:9092,lxe0965.allstate.com:9092";
    String schemaRegistryUrl = "http://lxe0961.allstate.com:8081";

    /**
     * Main method. Gets the Kafka Consumer and recieves events from Fact Span topic.
     *
     * @param args
     */
    public static void main(String[] args) {
        A1_KafkaConsumerAvro eventConsumer = new A1_KafkaConsumerAvro();
        KafkaConsumer consumer = eventConsumer.getConsumer();
        if (args[0].trim().equals("Alerts"))
            eventConsumer.consumeAlertEvents(consumer);
        else if (args[0].trim().equals("Span"))
            eventConsumer.consumeSpanEvents(consumer);
        else if (args[0].trim().equals("Stats"))
            eventConsumer.consumeStatsEvents(consumer);
        else
            System.out.println("Missing argument that specifies the event type! Exiting..");
        consumer.close();
    }

    /**
     * Creates a Kafka Consumer that can be used to receive messages from Kafka topic.
     *
     * @return
     */
    public KafkaConsumer getConsumer() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "VisionSpanDataProducer");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        configProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        configProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        configProperties.put("sasl.kerberos.service.name", "kafka");
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "statsConsumer");

        KafkaConsumer consumer = new KafkaConsumer<String, String>(configProperties);

        return consumer;
    }

    /**
     * Consumes the AVRO events from fact span events topic.
     *
     * @param consumer
     */
    public void consumeSpanEvents(KafkaConsumer consumer) {
        try {
            consumer.partitionsFor(factSpanTopic).stream().forEach(e -> System.out.println(e));

            consumer.subscribe(Collections.singletonList(factSpanTopic));
            System.out.println("Subscribed to topic " + factSpanTopic);

            while (true) {
                ConsumerRecords<Long, Object> records = consumer.poll(100);
                for (ConsumerRecord<Long, Object> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(),
                            record.key(),
                            record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Consumes the AVRO events from stats  topic.
     *
     * @param consumer
     */
    public void consumeStatsEvents(KafkaConsumer consumer) {
        try {
            consumer.partitionsFor(statsTopic).stream().forEach(e -> System.out.println(e));

            consumer.subscribe(Collections.singletonList(statsTopic));
            System.out.println("Subscribed to topic " + statsTopic);

            while (true) {
                ConsumerRecords<Long, Object> records = consumer.poll(100);
                for (ConsumerRecord<Long, Object> record : records) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Consumes the Alert events from Alerts topic.
     *
     * @param consumer
     */
    public void consumeAlertEvents(KafkaConsumer consumer) {
        try {
            consumer.partitionsFor(alertTopic).stream().forEach(e -> System.out.println(e));

            consumer.subscribe(Collections.singletonList(alertTopic));
            System.out.println("Subscribed to topic " + alertTopic);

            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(100);
                for (ConsumerRecord<String, Object> record : records) {
                    JSONObject currEvent = (JSONObject) (new JSONParser().parse(record.value().toString()));
                    System.out.printf("offset = %d\tkey = %s\tendpoint_id = %s\tzscore0To1 = %s\t" +
                                    "zscore1To2 = %s\tzscore2To3 = %s\tzscoreAbove3 = %s\terrors = %s\t" +
                                    "received_at = %s\traw_event = %s\n",
                            record.offset(),
                            record.key().trim(),
                            currEvent.get("endpoint_id"),
                            currEvent.get("zscore0To1"),
                            currEvent.get("zscore1To2"),
                            currEvent.get("zscore2To3"),
                            currEvent.get("zscoreAbove3"),
                            currEvent.get("errors"),
                            Instant.ofEpochMilli((long) currEvent.get("timestamp")).atZone(ZoneId.systemDefault()).toLocalDateTime(),
                            record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
