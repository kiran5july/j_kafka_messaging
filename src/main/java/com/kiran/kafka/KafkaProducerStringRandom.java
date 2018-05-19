package com.kiran.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaProducerStringRandom {
    private final static String TOPIC = "kafkatopic";
    private final static String BOOTSTRAP_SERVERS =     "localhost:9092"; //,localhost:9093,localhost:9094";

    /*
        private static Producer<Long, String> createProducer() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            return new KafkaProducer<>(props);
        }
    */
    public static void main(String[] argv)throws Exception {

        /*
        if (argv.length != 1) {
            System.err.println("Please pass topic name as 1st argument. ");
            System.exit(-1);
        }*/

        //String topicName = argv[0];

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer<String, String>(configProperties);
        Random rand = new Random();
        int randomInt = 0;
        for(int i=0; i<10; i++)
        {
            List meanList = Arrays.asList("0.7", "0.9", "1.1", "1.4", "1.5", "1.8");

            randomInt = rand.nextInt(meanList.size());
            //ProducerRecord<String, String> rec =
            System.out.print("Message sending: Key="+randomInt+ "; Value=Key"+randomInt+",Message"+randomInt) ;
            producer.send( new ProducerRecord<String, String>(TOPIC, "Key"+randomInt, "Key"+randomInt+",Message"+randomInt ) );
            System.out.println(" --> DONE");
        }
        producer.close();
    }
    public static String getDT(){
        long currentDateTime = System.currentTimeMillis();
        Date currentDate = new Date(currentDateTime);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(currentDate);
    }
}
