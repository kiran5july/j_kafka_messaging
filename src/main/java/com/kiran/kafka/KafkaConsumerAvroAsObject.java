package com.kiran.kafka;

//NOT WORKING YET

import com.allstate.bigdatacoe.vision.events.*;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafkaConsumerAvroAsObject {

    private final static String TOPIC = //"km.vision.endpointstats.topic.test2";
                                    //"rtalab.allstate.is.vision.stats";
                                    "rtalab.allstate.is.vision.stats";
    private final static String BOOTSTRAP_SERVERS =     //"localhost:9092";
            "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092, lxe0964.allstate.com:9092, lxe0965.allstate.com:9092";
    private final static String GROUP_ID = "KM_Consumer_Group3";
    //private logger = Logger.getLogger(this.getClass.getName)
    private final static String SCHEMA_REG_URL = "http://lxe0961.allstate.com:8081";

    public static void main(String[] argv)throws Exception {

        //String topicName = argv[0];

        long time = System.currentTimeMillis();


        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "Client_007");
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //From start of messages
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  //Default is True(means can't see messages after consumed )
        //configProperties.put("zookeeper.session.timeout.ms", "400");
        //configProperties.put("zookeeper.sync.time.ms", "200");
        //configProperties.put("auto.commit.interval.ms", "1000");
        configProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,  SCHEMA_REG_URL); //Not needed for ByteArrayDeser
        configProperties.put("sasl.kerberos.service.name", "kafka");
        configProperties.put("security.protocol", "SASL_PLAINTEXT");
        configProperties.put("failOnDataLoss", false);
        //configProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); //Gives ERROR
        configProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");

        org.apache.avro.Schema schema = com.kiran.utils.SchemaUtils.getStatsSchema();
        //Schema schema = Schema.Parser().parse(AvroSpecificDeserializer.class.getClassLoader().getResourceAsStream("avro/customer.avsc"));
        System.out.println("Schema= " + schema);

        //KafkaConsumer<String, GenericRecord> kafkaConsumer = new KafkaConsumer<String, GenericRecord>(configProperties);
        //KafkaConsumer<String, VisionStats> kafkaConsumer = new KafkaConsumer<String, VisionStats>(configProperties); //Reads as GenericData$Record
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(configProperties);
        //try (KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(configProperties)) {

        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

        //Start processing messages
        try {

            // Read using KafkaAvroDeserializer & GenericRecord
            while (true) {
                //try{
                VisionStats stats = null;
                GenericRecord genRec = null;
                    //System.out.println(getDT() + ": polling...");
                    ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(1000);
                    //ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(1000);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        if(record.key().equals("6163"))
                            System.out.println(getDT() + ": Message: Key=" + record.key() + "; value= " + record.value()); //It prints data
                        //=> Sample: 2018-05-18 14:39:51: Message: Key=1825; value= {"endpoint_id": 1825, "mean": 3964182.009408734, "stddev": 1631807.3477960108, "create_timestamp": 1526665899997}

                        //KafkaAvroDeserializer kdeser = new KafkaAvroDeserializer()
                        //VisionStats log = datumReader.read(null, binaryDecoder);
                        //stats = record.value().getClass().cast(VisionStats.class);
                        try {
                            String s = "hello";
                            //datumReader.read(genRec , binaryDecoder);
                            //System.out.println(getDT() + ": Message: Key=" + record.key() + "; value= " + stats.endpoint_id);

                        }catch(Exception ex){
                            System.out.println("Exception in for: " + ex.getMessage());
                        }
                    }
                //}catch(Exception ex){
                //    System.out.println("Exception in while: " + ex.getMessage());
                //    ex.printStackTrace();
                //}
            }

        }catch(WakeupException ex){
            System.out.println("Exception caught " + ex.getMessage());
        }finally{
            kafkaConsumer.close();
            System.out.println("After closing KafkaConsumer");
        }

    }

    public static String getDT(){
        long currentDateTime = System.currentTimeMillis();
        Date currentDate = new Date(currentDateTime);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(currentDate);
    }

}
