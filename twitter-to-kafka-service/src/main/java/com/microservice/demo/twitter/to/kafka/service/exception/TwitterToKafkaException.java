package com.microservice.demo.twitter.to.kafka.service.exception;

public class TwitterToKafkaException extends RuntimeException{

    public TwitterToKafkaException() { super();}

    public TwitterToKafkaException(String msg) { super(msg);}

    public TwitterToKafkaException(String msg, Throwable t) { super(msg, t);}
}
