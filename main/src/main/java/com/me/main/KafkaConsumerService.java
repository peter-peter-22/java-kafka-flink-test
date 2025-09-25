package com.me.main;

import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = "test-topic", groupId = "consumer-local")
    public void consume(String message) {
        logger.info("Received message: {}", message);
    }
}