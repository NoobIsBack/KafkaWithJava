package com.mayank.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
/*
* By providing a key we make sure that we keys goes to same partition
* */
public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        System.out.println("first Kafka Project");

        Logger logger = LoggerFactory.getLogger(Producer.class);

//        Create properties file
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0;i < 10; i++) {

            String topic = "first_topic";
            String value = "hello world" + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    topic, key, value);

//        send data async
            producer.send(record);
            System.out.println("Key " + key);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
//                    the record was successfully sent
                        System.out.println("Received data: \n" + " Topic " + recordMetadata.topic() +
                                "\n Partition " + recordMetadata.partition() +
                                "\n + offset: " + recordMetadata.offset() +
                                "\n Timestamp " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing Data", e);
                    }
                }
            }).get();   //block the send to make it sync
        }

//        flush data
        producer.flush();

//        flush and close producer
        producer.close();
    }
}
