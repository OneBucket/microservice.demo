package com.microservice.demo.kafka.admin.client;

import com.microservice.demo.config.KafkaConfigData;
import com.microservice.demo.config.RetryConfigData;
import com.microservice.demo.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    private final WebClient webClient;


    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopic);
            LOG.info("create topics {}", createTopicsResult.values().values());

        } catch (Throwable t) {
            throw new KafkaClientException("Reach max retry count for creating kafka topics", t);
        }
        checkCreatedTopics();
    }

    private CreateTopicsResult doCreateTopic(RetryContext context) {
        List<String> topicsName = kafkaConfigData.getTopicNameToCreate();
        LOG.info("creating {} topics , attemps {}", topicsName.size(), context.getRetryCount());
        List<NewTopic> kafkaTopics = topicsName.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getRepicationFactor()))
                .collect(Collectors.toList());
        return adminClient.createTopics(kafkaTopics);

    }

    private void checkCreatedTopics() {
        int maxRetryCount = retryConfigData.getMaxAttempts();
        int retryCount = 1;
        int mutiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        Collection<TopicListing> createdTopics = getTopic();
        for(String topic : kafkaConfigData.getTopicNameToCreate()) {
            while (!isTopicCreated(createdTopics, topic)) {
                checkMaxRetry(retryCount, maxRetryCount);
                retryCount ++;
                sleepTimeMs *= mutiplier;
                sleep(sleepTimeMs);
                createdTopics = getTopic();
            }
        }


    }

    private void checkMaxRetry(int retryCount, int maxRetryCount) {
        if(retryCount > maxRetryCount)
        {
            throw new KafkaClientException("reach max retry count");
        }


    }

    private void sleep(Long time) {
        try {
            Thread.sleep(time);
        } catch (Throwable t) {
            throw new KafkaClientException("Problem appeared while sleeping", t);
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> createdTopics,String topic) {
        if(createdTopics == null || createdTopics.isEmpty()) {
            return false;
        }
        return createdTopics.stream().anyMatch(topicName -> topicName.name().equals(topic));
    }

    private Collection<TopicListing> getTopic() {
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopic);
        } catch (Throwable t) {
            throw new KafkaClientException("reached max retry count getting topics", t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopic(RetryContext context) throws ExecutionException, InterruptedException {
        LOG.info("try to get Kafka topics {}, attempt {}", kafkaConfigData.getTopicNameToCreate().toArray(), context.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if(topics != null) {
            topics.forEach(topic -> LOG.info("get topic with name {}", topic.name()));
        }
        return topics;

    }

    public void checkSchemaRegistry() {
        int maxRetryCount = retryConfigData.getMaxAttempts();
        //int retryCount = 1;
        int mutiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while (!getSchemaRegistryStatus().is2xxSuccessful()){
            for(int retryCount = 1; retryCount <= retryConfigData.getMaxAttempts(); retryCount ++) {
                sleepTimeMs *= mutiplier;
                sleep(sleepTimeMs);
          }
        }
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient.method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegisryUrl())
                    .exchangeToMono(response -> Mono.just(response.statusCode()))
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

}
