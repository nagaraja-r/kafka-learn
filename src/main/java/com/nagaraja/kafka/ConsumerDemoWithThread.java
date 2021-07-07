package com.nagaraja.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    private ConsumerDemoWithThread() {

    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");

        //create the consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, groupId, topic, bootstrapServer);

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            //wait until all the tasks are done (count = 0)
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing..");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch,
                                String groupId,
                                String topic,
                                String bootstrapServer) {
            this.latch = latch;

            //create a consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
                    , StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    "earliest");
            this.consumer = new KafkaConsumer<>(properties);
            //subscribe consumer to topics
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code that we are done with consumer
                latch.countDown();
            }

        }

        public void shutDown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw an exception WakeUpException
            consumer.wakeup();
        }
    }
}
