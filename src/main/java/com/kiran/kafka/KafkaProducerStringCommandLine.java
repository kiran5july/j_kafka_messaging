package com.kiran.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerStringCommandLine {
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
        Scanner in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);

        String line = in.nextLine();
        while( ! line.equals("exit") ) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(TOPIC, line.split(",")[0], line);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }
    public static String getDT(){
        long currentDateTime = System.currentTimeMillis();
        Date currentDate = new Date(currentDateTime);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(currentDate);
    }
}
