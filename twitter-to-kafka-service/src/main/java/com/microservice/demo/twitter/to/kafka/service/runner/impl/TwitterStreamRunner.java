package com.microservice.demo.twitter.to.kafka.service.runner.impl;


import com.microservice.demo.config.TwitterToKafkaConfigData;
import com.microservice.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.TwitterException;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URISyntaxException;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(StreamRunner.class);

    private final TwitterStreamHelper twitterStreamHelper;

    private final TwitterToKafkaConfigData twitterToKafkaConfigData;

    public TwitterStreamRunner(TwitterStreamHelper twitterStreamHelper, TwitterToKafkaConfigData twitterToKafkaConfigData) {
        this.twitterStreamHelper = twitterStreamHelper;
        this.twitterToKafkaConfigData = twitterToKafkaConfigData;
    }

    @Override
    public void start() throws TwitterException {
        String bearerToken = twitterToKafkaConfigData.getTwitterV2BearerToken();
        if (bearerToken != null) {
            try {
                twitterStreamHelper.setupRules(bearerToken, twitterStreamHelper.getRules());
                twitterStreamHelper.connectStream(bearerToken);
            } catch (IOException | URISyntaxException e) {
                LOG.error("Error streaming tweet", e);
                throw new RuntimeException("Error streaming tweet", e);
            }

        }


    }
}

