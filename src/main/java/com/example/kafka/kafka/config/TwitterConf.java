package com.example.kafka.kafka.config;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
public class TwitterConf {

    @Value("${twitter.api.consumer.key}")
    private String consumerKey;

    @Value("${twitter.api.consumer.secret}")
    private String consumerSecret;

    @Value("${twitter.api.access.token}")
    private String accessToken;

    @Value("${twitter.api.access.secret}")
    private String accessSecret;

    private static final String TERM_TWITTER = "#twitter";
    private static final List<String> TERMS = Lists.newArrayList(TERM_TWITTER);

    static final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(2000);
    static final StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();


    @Bean
    public Client getClient() {
        endpoint.trackTerms(TERMS);
        final Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessSecret);
        return new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }
}
