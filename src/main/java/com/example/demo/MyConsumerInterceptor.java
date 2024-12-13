package com.example.demo;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class MyConsumerInterceptor<T> implements ConsumerInterceptor<T> {

    private static final Logger log = LoggerFactory.getLogger(MyConsumerInterceptor.class);

    @Override
    public void close() {
        log.info("MyConsumerInterceptor close");
    }

    @Override
    public Message<T> onArrival(Consumer<T> consumer, Message<T> message) {
        log.info("MyConsumerInterceptor onArrival");
        return message;
    }

    @Override
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
        log.info("MyConsumerInterceptor beforeConsume");
        return message;
    }

    @Override
    public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        log.info("MyConsumerInterceptor onAcknowledge");
    }

    @Override
    public void onAcknowledgeCumulative(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        log.info("MyConsumerInterceptor onAcknowledgeCumulative");
    }

    @Override
    public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        log.info("MyConsumerInterceptor onNegativeAcksSend");
    }

    @Override
    public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        log.info("MyConsumerInterceptor onAckTimeoutSend");
    }

    @Override
    public void onPartitionsChange(String topicName, int partitions) {
        log.info("MyConsumerInterceptor onPartitionsChange");
    }
}
