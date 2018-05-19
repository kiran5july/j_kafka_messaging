package com.kiran.kafka;


/*
    Run the program to start Consumer. First start producer & input a message.
    If message is in format (key1,message1): key= key1, message = message1
    else if simgple string (message2): key=message2, message=message2


 */
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.Arrays;
import org.apache.kafka.common.errors.*;

public class KafkaConsumerStringUsingThread {

    private final static String BOOTSTRAP_SERVERS =     //"localhost:9092";
            "lxe0961.allstate.com:9092, lxe0962.allstate.com:9092, lxe0963.allstate.com:9092, lxe0964.allstate.com:9092, lxe0965.allstate.com:9092";
    private final static String TOPIC = //"kafkatopic";
            "rtalab.allstate.is.vision.stats";
    private final static String GROUP_ID = "KM_Consumer_Group3";
    private static Scanner in;
    private static boolean stop = false;

    public static void main(String[] argv)throws Exception{

        /*
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    Consumer.class.getSimpleName());
            System.exit(-1);
        }
        //String topicName = argv[0];
        //String groupId = argv[1];
        */

        in = new Scanner(System.in);

        KafkaConsumerThread consumerRunnable = new KafkaConsumerThread(TOPIC, GROUP_ID);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class KafkaConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,String> kafkaConsumer;

        public KafkaConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //From start of messages
            configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  //Default is True(means can't see messages after consumed )
            configProperties.put("sasl.kerberos.service.name", "kafka"); //Kerberos
            configProperties.put("security.protocol", "SASL_PLAINTEXT"); //Kerberos
            //configProperties.put("failOnDataLoss", false);
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);

            kafkaConsumer.subscribe(Arrays.asList(topicName));
            //Start processing messages
            try {

                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                        System.out.println(getDT() + ": Message: Key=" + record.key() + "; value= "+record.value());


                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,String> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
    public static String getDT(){
        long currentDateTime = System.currentTimeMillis();
        Date currentDate = new Date(currentDateTime);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(currentDate);
    }
}
