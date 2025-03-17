package com.practice.kafka.consumer;

import com.practice.kafka.model.OrderModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class OrderSerdeConsumer<K extends Serializable, V extends Serializable> {

    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topics;

    public OrderSerdeConsumer(Properties consumerProps, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<>(consumerProps);
        this.topics = topics;
    }

    public void initConsumer() {
        this.kafkaConsumer.subscribe(topics);
        addShutdownHookToRuntime(kafkaConsumer);
    }

    private void addShutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        Thread currentThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("main program start to exit by calling wakeup");
            kafkaConsumer.wakeup();
            try {
                currentThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    public static void main(String[] args) {
        String topicName=  "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-serde-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        OrderSerdeConsumer<String, OrderModel> baseConsumer = new OrderSerdeConsumer<String, OrderModel>(props, List.of(topicName));
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
            log.error("wakeup exception has been called");
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            log.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            log.info("finally consumer is closing");
            closeConsumer();
        }
    }

    private void pollCommitAsync(long durationMillis) {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                log.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
            }
        });
    }

    private void pollCommitSync(long durationMillis) throws WakeupException {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if (consumerRecords.count() > 0) {
                kafkaConsumer.commitSync();
                log.info("commit sync has been called");
            }
        } catch (CommitFailedException e) {
            log.error(e.getMessage());
        }
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        log.info("record key:{},  partition:{}, record offset:{} record value:{}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        records.forEach(record -> processRecord(record));
    }

    public void closeConsumer() {
        kafkaConsumer.close();
    }


}
