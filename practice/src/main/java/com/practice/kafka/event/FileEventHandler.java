package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

}
