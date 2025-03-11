package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);
    private KafkaProducer<String, String> producer;
    private String topicName;
    private boolean sync;

    public FileEventHandler(KafkaProducer<String, String> producer, String topicName, boolean sync) {
        this.producer = producer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);
        if (this.sync) {
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            logRecordMetadata(recordMetadata, null);
        } else {
            producer.send(producerRecord, this::logRecordMetadata);
        }
    }

    private void logRecordMetadata(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            logger.info("\n ##### record metadata received ##### \n" +
                    "partition:" + metadata.partition() + "\n" +
                    "offset:" + metadata.offset() + "\n" +
                    "timestamp:" + metadata.timestamp() + "\n");
        } else {
            logger.error("exception error from broker " + exception.getMessage());
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        boolean sync = true;

        FileEventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
        eventHandler.onMessage(messageEvent);
    }

}
