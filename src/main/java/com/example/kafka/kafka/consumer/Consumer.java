package com.example.kafka.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Consumer {

    @KafkaListener(topics = "api-twitter-topic")
    public void processMessage(String message) {
        log.info("Message - {}", message);
    }
}
