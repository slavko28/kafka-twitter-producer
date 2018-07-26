package com.example.kafka.kafka.producer;


import com.example.kafka.kafka.config.TwitterConf;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//import org.springframework.social.twitter.api.impl.TwitterTemplate;

@Slf4j
@Component
public class Producer {

    @Value("${twitter.api.consumer.key}")
    private String consumerKey;

    @Value("${twitter.api.consumer.secret}")
    private String consumerSecret;

    @Value("${twitter.api.access.token}")
    private String accessToken;

    @Value("${twitter.api.access.secret}")
    private String accessSecret;

    @Value("${twitter.api.default.capacity}")
    private int twitterApiDefaultCapacity;

    private static final String TOPIC_NAME = "api-twitter-topic";
    private static final int MESSAGE_LIMIT = 1000;
    static final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(2000);

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ClientBuilder clientBuilder;

    @Autowired
    public Producer(KafkaTemplate<String, String> kafkaTemplate, ClientBuilder clientBuilder) {
        this.kafkaTemplate = kafkaTemplate;
        this.clientBuilder = clientBuilder;
    }

    @PostConstruct
    public void run(String message) {
        Client client = clientBuilder.processor(new StringDelimitedProcessor(queue)).build();
        client.connect();

        for (int msgRead = 0; msgRead < MESSAGE_LIMIT; msgRead++) {
            try {
                final String payload = queue.take();
                kafkaTemplate.send(new ProducerRecord<>(TOPIC_NAME, payload));
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.stop();
    }

}
