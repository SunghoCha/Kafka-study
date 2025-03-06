package com.app.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerAsyncWithKey {
    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithKey.class);
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

        for(int seq = 0; seq < 2000; seq++) {
            // ProducerRecord 객체 생성
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world" + seq);

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

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();

    }
}
