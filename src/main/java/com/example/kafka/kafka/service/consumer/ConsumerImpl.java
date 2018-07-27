package com.example.kafka.kafka.service.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerImpl implements ConsumerService {

    @Override
    @KafkaListener(topics = "${kafka.topic}")
    public void processMessage(String message) {
        log.info("Message - {}", message);
    }
}
