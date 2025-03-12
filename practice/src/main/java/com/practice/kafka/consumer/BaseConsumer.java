package com.practice.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BaseConsumer<K extends Serializable, V extends Serializable> {
    public static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class.getName());
    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topics;

    public BaseConsumer(Properties consumerProps, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<>(props, List.of(topicName));
        baseConsumer.initConsumer();
        String commitMode = "async";

        baseConsumer.pollConsumes(100, commitMode);
        baseConsumer.closeConsumer();
    }

    private void pollConsumes(long durationMillis, String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        } catch (WakeupException e) {
            logger.error("wake execption has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("#### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            closeConsumer();
        }
    }

    private void pollCommitAsync(long durationMillis) throws WakeupException{
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        kafkaConsumer.commitAsync(((offsets, exception) -> {
            if (exception != null) {
                logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
            }
        }));
    }

    private void pollCommitSync(long durationMillis) throws WakeupException {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if (consumerRecords.count() > 0) {
                kafkaConsumer.commitSync();
                logger.info("commit sync completed");
            }
        } catch (CommitFailedException e) {
            logger.error(e.getMessage());
        }
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::processRecord);
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void closeConsumer() {
        kafkaConsumer.close();
    }

    private void initConsumer() {
        kafkaConsumer.subscribe(topics);
        addShutdownHookToRuntime(kafkaConsumer);
    }

    private void addShutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        Thread currentThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();
            try {
                currentThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

}
