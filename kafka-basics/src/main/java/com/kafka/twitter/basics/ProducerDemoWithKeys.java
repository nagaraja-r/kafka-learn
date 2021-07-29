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
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        // create a Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String,
                String>(properties);


        int i = 0;
        while (i < 10) {

            // create a Producer Record
            String topic = "first_topic";
            String value = "Hello World " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(
                            topic,
                            key,
                            value);

            logger.info("Key : " + key);

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
            }).get(); // block .send() to make synchronous
            //flush data
            producer.flush();
            i++;

        }
        producer.close();
    }
}
