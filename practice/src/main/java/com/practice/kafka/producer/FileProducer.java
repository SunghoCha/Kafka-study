package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.stream.IntStream;

public class FileProducer {
    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class);
    public static void main(String[] args) {
        String topicName = "file-topic";

        // 카프카 config 설정
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        String filePath = "F:\\git\\KafkaProj-01\\practice\\src\\main\\resources\\pizza_sample.txt";

        // KafkaProducer 객체 생성 -> ProducerRecords 생성 -> send() 비동기 방식 전송
        sendFileMessages(kafkaProducer, topicName, filePath);


        kafkaProducer.close();

    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimiter = ",";
        try {
            FileReader reader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(reader);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0].trim();

                StringJoiner valueJoiner = new StringJoiner(delimiter);
                IntStream.range(0, tokens.length).forEach(i -> valueJoiner.add(tokens[i].trim()));
                String value = valueJoiner.toString();

                sendMessage(kafkaProducer, topicName, key, value);
            }
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, String> producer, String topicName, String key, String value) {
        // ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        logger.info("key:{}, value:{}", key, value);

        // KafkaProducer 메시지 send
        producer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("\n ##### record metadata received ##### \n" +
                                "partition:" + metadata.partition() + "\n" +
                                "offset:" + metadata.offset() + "\n" +
                                "timestamp:" + metadata.timestamp() + "\n");
                    } else {
                        logger.error("exception error from broker " + exception.getMessage());
                    }
                }
        );
    }
}
