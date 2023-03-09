package com.microservice.demo.service.Impl;

import com.microservice.demo.service.KafkaProducer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Component
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOG.info("message {} send to topic {}", message.toString(), topicName);
        ListenableFuture<SendResult<Long, TwitterAvroModel>> future =
                kafkaTemplate.send(topicName, key, message);
        addCallBack(topicName, message, future);

    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOG.info("close kafka producer.");
            kafkaTemplate.destroy();
        }
    }

    private void addCallBack(String topicName, TwitterAvroModel message,
                             ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaListenableFuture) {
        kafkaListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
            @Override
            public void onFailure(Throwable ex) {
                LOG.error("error while send message {} to topic {}", message.toString(), topicName);
            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
                RecordMetadata metadata = result.getRecordMetadata();
                LOG.debug("Received new metadata. Topic: {}; Partition {}; Offset {}; Timestamp {}, at time {}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        System.nanoTime());

            }
        });
    }
}
