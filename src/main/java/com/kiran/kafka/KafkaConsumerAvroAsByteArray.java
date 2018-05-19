package com.kiran.kafka;
//WORKS BUT GIVES WRONG DATA

//import io.confluent.kafka.serializers.*;

import com.kiran.utils.SchemaUtils;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.avro.generic.GenericDatumReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class KafkaConsumerAvroAsByteArray {

    private final static String TOPIC = //"km.vision.endpointstats.topic.test2";
                                    "rtalab.allstate.is.vision.stats";
                                    //"rtalab.allstate.is.vision.test";
    private final static String BOOTSTRAP_SERVERS =     //"localhost:9092";
            "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092, lxe0964.allstate.com:9092, lxe0965.allstate.com:9092";
    private final static String GROUP_ID = "KM_Consumer_Group3";
    //private logger = Logger.getLogger(this.getClass.getName)
    private final static String SCHEMA_REG_URL = "http://lxe0961.allstate.com:8081";

    public static void main(String[] argv)throws Exception {

        /*
        if (argv.length != 1) {
            System.err.println("Please pass topic name as 1st argument. ");
            System.exit(-1);
        }*/

        //String topicName = argv[0];
        //private Kryo kryo = new Kryo();
        long time = System.currentTimeMillis();


        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer"); //Serdes.ByteArraySerde.class - NOT WORKING
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "Client-007");
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //From start of messages
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  //Default is True(means can't see messages after consumed )
        //configProperties.put("zookeeper.session.timeout.ms", "400");
        //configProperties.put("zookeeper.sync.time.ms", "200");
        //configProperties.put("auto.commit.interval.ms", "1000");
        configProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,  SCHEMA_REG_URL); //Not needed for ByteArrayDeser
        configProperties.put("sasl.kerberos.service.name", "kafka");
        configProperties.put("security.protocol", "SASL_PLAINTEXT");
        configProperties.put("failOnDataLoss", false);
        //configProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");

        org.apache.avro.Schema schema = com.kiran.utils.SchemaUtils.getStatsSchema();

        //String userSchema = "";
        //Schema.Parser parser = new Schema.Parser();
        //Schema schema = parser.parse(userSchema);

        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(configProperties);

        //org.apache.avro.Schema schema = SchemaUtils.getVisionEventStatsSchema();
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

        //Start processing messages
        try {
            GenericRecord genericRecord = null;
            // Read as Byte array
            while (true) {
                //System.out.println(getDT() + ": polling...");
                ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    if(record.key().equals("6163")) {
                        System.out.println(getDT() + ": Message: Key=" + record.key() + "; value= " + record.value()); //Prints Bytes


                        //Manually decode the byte array --NOT WORKING YET
                        //BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(record.value()), null); //binaryDecoder
                        //GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                        //genericRecord = reader.read(genericRecord, decoder);
                        try {

                            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                            genericRecord = reader.read(null, DecoderFactory.get().binaryDecoder(record.value(), null));
                            //DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(record.value()), reader);

                            System.out.println(getDT() + ": Avro record: " + genericRecord.toString() );
                            System.out.println(getDT() + ": Data: " + genericRecord.get("endpoint_id")+"; "+genericRecord.get("mean") );  //Gives me wrong data

                        }catch (SerializationException e) {
                            //kafkaConsumer.seek(e.partition(), e.offset() + 1);
                        } catch (Exception e) {
                            System.out.println("Exception in decoding: " + e.getMessage());
                        } finally {
                        }

                    }
                }

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
