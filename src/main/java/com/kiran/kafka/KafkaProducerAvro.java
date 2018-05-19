package com.kiran.kafka;


//import io.confluent.kafka.serializers.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import com.kiran.utils.*;
import java.io.ByteArrayOutputStream;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.commons.codec.binary.Hex;
//import kafka.producer.KeyedMessage;

public class KafkaProducerAvro {

    private final static String TOPIC = "km.vision.endpointstats.topic.test2";
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

        long time = System.currentTimeMillis();

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        //configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        Producer producer = new KafkaProducer<String, String>(configProperties);

        org.apache.avro.Schema schema = com.kiran.utils.SchemaUtils.getStatsSchema();
        Random rand = new Random();
        int randomInt = 0;
        for(int i=0; i<10; i++)
        {
            List meanList = Arrays.asList("0.7", "0.9", "1.1", "1.4", "1.5", "1.8");
            List stdevList = Arrays.asList("0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8");

            GenericRecord rec = new GenericData.Record(schema);
            rec.put("endPoint_id", Long.parseLong(""+i));
            rec.put("mean", Double.parseDouble(meanList.get(rand.nextInt(meanList.size())).toString()));
            rec.put("stdev", Double.parseDouble(stdevList.get(rand.nextInt(stdevList.size())).toString()));

            ByteArrayOutputStream baoStrm = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(baoStrm, null);
            binaryEncoder.flush();

            DatumWriter<GenericRecord>writer = new SpecificDatumWriter<GenericRecord>(schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(rec, encoder);
            encoder.flush();
            out.close();

            try {
                System.out.println("Message sending: Key="+Long.parseLong(""+i)+ "; AvroRecord: --") ;
                //ProducerRecord<Long, GenericRecord> record = new ProducerRecord<Long, GenericRecord>(TOPIC, Long.parseLong(""+i), rec);
                producer.send( new ProducerRecord<Long, byte[]>(TOPIC, Long.parseLong(""+i), out.toByteArray()) );

                //TEST: Hex String
                //byte[] serializedBytes = out.toByteArray();
                //System.out.println("Message sending: Key="+Long.parseLong(""+i)+ "; AvroRecord bytes : " + serializedBytes);
                //String serializedHex = Hex.encodeHexString(serializedBytes);
                //System.out.println("Serialized Hex String : " + serializedHex);
                //KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>("page_views", serializedBytes);


                System.out.println(" --> DONE");
            } catch (Exception e) {
                e.printStackTrace();
            }

            //ProducerRecord<String, String> rec =

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
