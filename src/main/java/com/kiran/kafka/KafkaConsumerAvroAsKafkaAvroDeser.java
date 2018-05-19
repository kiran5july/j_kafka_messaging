package com.kiran.kafka;
//NOT WORKING YET

//import com.allstate.bigdatacoe.vision.events.VisionStats;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import io.confluent.kafka.serializers.*;

public class KafkaConsumerAvroAsKafkaAvroDeser {

    private final static String TOPIC = //"km.vision.endpointstats.topic.test2";
            "rtalab.allstate.is.vision.stats";
    private final static String BOOTSTRAP_SERVERS =     //"localhost:9092";
            "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092, lxe0964.allstate.com:9092, lxe0965.allstate.com:9092";
    private final static String GROUP_ID = "KM_Consumer_Group3";
    //private logger = Logger.getLogger(this.getClass.getName)
    private final static String SCHEMA_REG_URL = "http://lxe0961.allstate.com:8081";

    public static void main(String[] argv)throws Exception {

        long time = System.currentTimeMillis();

        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "Client_007");
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //From start of messages
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  //Default is True(means can't see messages after consumed )
        //configProperties.put("zookeeper.session.timeout.ms", "400");
        //configProperties.put("zookeeper.sync.time.ms", "200");
        //configProperties.put("auto.commit.interval.ms", "1000");
        //configProperties.put("schema.registry.url", SCHEMA_REG_URL);
        //configProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); //IMP: Automatically deserializes to class as in message
        configProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REG_URL);
        configProperties.put("sasl.kerberos.service.name", "kafka");
        configProperties.put("security.protocol", "SASL_PLAINTEXT");
        configProperties.put("failOnDataLoss", false);
        //configProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");

/*
        SchemaRegistryClient SchemaRegistryClient = new CachedSchemaRegistryClient(    configProperties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG),1000);
        KafkaAvroDeserializer kAvroDeser = new KafkaAvroDeserializer(SchemaRegistryClient);
        Map<String, String> configSchema = new HashMap<String, String>();
        configSchema.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REG_URL);
        kAvroDeser.configure(configSchema, false);
        //System.out.println("SchemaReg="+ kAvroDeser.toString());
*/

        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(configProperties);
        //KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties);

        //org.apache.avro.Schema schema = com.kiran.utils.SchemaUtils.getStatsSchema();
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));
        //Start processing messages
        try {

            //GenericData.Record record = new GenericData.Record( com.kiran.utils.SchemaUtils.getStatsSchema() );

            while (true) {
                System.out.println(getDT() + ": polling...");
                ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(1000);

                if (records.count() == 0) {
                    System.out.println(getDT()+": No message from topic.");
                } else {
                    //records.forEach(record -> {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        System.out.println(getDT() + ": Message: Key=" + record.key() + "; value= " + record.value());
                        //System.out.printf(getDT() + ": %s %d %d ; Key=%s; value=%s \n", record.topic(), record.partition(), record.offset(), record.key(), record.value());


                        //GenericRecord genRec = (GenericRecord) record.value();
                        //System.out.println("Schema:" + genRec.getSchema());

                        //System.out.printf(getDT()+": endpoint_id=%s \n", recSpanStats.endpoint_id);
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

