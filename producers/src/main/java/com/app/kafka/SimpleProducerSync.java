package com.app.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);
    public static void main(String[] args) {
        String topicName = "simple-topic";

        // 카프카 config 설정
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world4");

        // KafkaProducer 메시지 send
        try {
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            logger.info("\n ##### record metadata received ##### \n" +
                    "partition:"  + recordMetadata.partition() + "\n" +
                    "offset:"  + recordMetadata.offset() + "\n" +
                    "timestamp:"  + recordMetadata.timestamp() + "\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}
