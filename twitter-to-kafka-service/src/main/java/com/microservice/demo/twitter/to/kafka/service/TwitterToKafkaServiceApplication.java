package com.microservice.demo.twitter.to.kafka.service;


import com.microservice.demo.config.TwitterToKafkaConfigData;
import com.microservice.demo.twitter.to.kafka.service.init.StreamInitializer;
import com.microservice.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import twitter4j.TwitterException;


@SpringBootApplication
@ComponentScan(basePackages = "com.microservice.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
    //implements CommandLineRunner with run method to initialize automatically when run springbootapplication
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
    private final TwitterToKafkaConfigData twitterToKafkaConfigData;

    private final StreamRunner streamRunner;

    private final StreamInitializer streamInitializer;

    public TwitterToKafkaServiceApplication(TwitterToKafkaConfigData twitterToKafkaConfigData, StreamRunner streamRunner, StreamInitializer streamInitializer) {
        //using constructor rather than annotaion:
        //1.can set a final object which is immutable and thread safe
        //2.without using reflection, can be faster
        //3.force to create required dependency, here is twitterToKafkaConfigData
        this.twitterToKafkaConfigData = twitterToKafkaConfigData;

        this.streamRunner = streamRunner;


        this.streamInitializer = streamInitializer;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws TwitterException {
        LOG.info("start twitter to kafka application");
        LOG.info(twitterToKafkaConfigData.getTwitterKeywords().toString());
        LOG.info(twitterToKafkaConfigData.getWelcomeMessage().toString());
        streamInitializer.init();
        streamRunner.start();


    }
}
