package com.mayank.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
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

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    "first_topic", "java programming Again Learning " + Integer.toString(i));

//        send data async
            producer.send(record);

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
            });
        }

//        flush data
        producer.flush();

//        flush and close producer
        producer.close();
    }
}
