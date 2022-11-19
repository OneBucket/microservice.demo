package com.microservice.demo.twitter.to.kafka.service;

import com.microservice.demo.twitter.to.kafka.service.config.TwitterToKafkaConfigData;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;




@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
    //implements CommandLineRunner with run method to initialize automatically when run springbootapplication
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
    private final TwitterToKafkaConfigData twitterToKafkaConfigData;

    public TwitterToKafkaServiceApplication(TwitterToKafkaConfigData twitterToKafkaConfigData) {
        //using constructor rather than annotaion:
        //1.can set a final object which is immutable and thread safe
        //2.without using reflection, can be faster
        //3.force to create required dependency, here is twitterToKafkaConfigData
        this.twitterToKafkaConfigData = twitterToKafkaConfigData;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) {
        LOG.info("start twitter to kafka application");
        LOG.info(twitterToKafkaConfigData.getTwitterKeywords().toString());
        LOG.info(twitterToKafkaConfigData.getWelcomeMessage().toString());


    }
}
