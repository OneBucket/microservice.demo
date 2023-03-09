package com.microservice.demo.twitter.to.kafka.service.listener;

import com.microservice.demo.config.KafkaConfigData;
import com.microservice.demo.service.KafkaProducer;
import com.microservice.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterStatusListener extends StatusAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterStatusListener.class);

    private final KafkaConfigData kafkaConfigData;
    private final TwitterStatusToAvroTransformer transformer;

    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

    public TwitterStatusListener(KafkaConfigData kafkaConfigData, TwitterStatusToAvroTransformer transformer, KafkaProducer<Long, TwitterAvroModel> kafkaProducer) {
        this.kafkaConfigData = kafkaConfigData;
        this.transformer = transformer;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onStatus(Status status) {
        //LOG.info("Twitter status : {}", status.getText());
        LOG.info("Receive data {}, send to Kafka Topics {}", status.getText(), kafkaConfigData.getTopicName());
        TwitterAvroModel twitterAvroModel = transformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);


    }
}
