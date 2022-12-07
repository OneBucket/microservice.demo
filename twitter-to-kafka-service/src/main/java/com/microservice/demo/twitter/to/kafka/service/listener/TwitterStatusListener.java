package com.microservice.demo.twitter.to.kafka.service.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.StatusAdapter;

public class TwitterStatusListener extends StatusAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterStatusListener.class);
    @Override
    public void onStatus(Status status) {
        LOG.info("Twitter status : {}", status.getText());
    }
}
