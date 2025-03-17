package com.practice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.practice.kafka.model.OrderModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class OrderDeserializer implements Deserializer<OrderModel> {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public OrderModel deserialize(String topic, byte[] data) {
        OrderModel orderModel = null;
        try {
            orderModel = objectMapper.readValue(data, OrderModel.class);
        } catch (IOException e) {
            log.error("Object mapper deserialize error: {}", e.getMessage());
        }
        return orderModel;
    }
}
