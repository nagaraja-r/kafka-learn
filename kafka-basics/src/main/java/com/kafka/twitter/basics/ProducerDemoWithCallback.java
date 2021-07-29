package com.kafka.twitter.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        Logger logger =
                LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        // create a Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        int i = 0;
        while (i < 100000) {
            // create a Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(
                            "first_topic",
                            "Hello World" + new Random());

            // send data - asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata,
                                         Exception e) {
                    // executes everytime a record is successfully sent or an
                    // exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata :\n" +
                                "topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() +
                                "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }

                }
            });
            //flush data
            producer.flush();
            i++;
        }
        producer.close();


    }
}
