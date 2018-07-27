package com.example.kafka.kafka.service.producer;


import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Service
public class ProducerImpl implements ProducerService {

    @Value("${kafka.topic}")
    private String topicName;

    private Client client;

    private static final int MESSAGE_LIMIT = 1000;

    static final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(2000);

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ClientBuilder clientBuilder;

    @Autowired
    public ProducerImpl(KafkaTemplate<String, String> kafkaTemplate, ClientBuilder clientBuilder) {
        this.kafkaTemplate = kafkaTemplate;
        this.clientBuilder = clientBuilder;
    }

    @Override
    public void run() {
        client = clientBuilder.processor(new StringDelimitedProcessor(queue)).build();
        client.connect();

        for (int msgRead = 0; msgRead < MESSAGE_LIMIT; msgRead++) {
            try {
                final String payload = queue.take();
                kafkaTemplate.send(new ProducerRecord<>(topicName, payload));
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        client.stop();
    }

}
