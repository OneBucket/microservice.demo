package com.microservice.demo.twitter.to.kafka.service.init.impl;

import com.microservice.demo.config.KafkaConfigData;
import com.microservice.demo.kafka.admin.client.KafkaAdminClient;
import com.microservice.demo.twitter.to.kafka.service.init.StreamInitializer;
import com.microservice.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializer implements StreamInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamInitializer.class);

    private final KafkaAdminClient adminClient;

    private final KafkaConfigData configData;

    public KafkaStreamInitializer(KafkaAdminClient adminClient, KafkaConfigData configData) {
        this.adminClient = adminClient;
        this.configData = configData;
    }


    @Override
    public void init() {
        adminClient.createTopics();
        adminClient.checkSchemaRegistry();
        LOG.info("Topic with names {} is creates successfully", configData.getTopicNameToCreate().toArray());



    }
}
