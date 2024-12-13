package com.example.demo;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ConsumerInterceptorDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerInterceptorDemo.class);

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient pulsarClient = PulsarClient.builder()
                .memoryLimit(10, SizeUnit.KILO_BYTES)
                .serviceUrl("pulsar://localhost:6650")
                .build();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("consumer-interceptor")
                .subscriptionName("sub1")
                .intercept(new MyConsumerInterceptor<>())
                .subscribe();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                consumer.close();
                pulsarClient.close();
            } catch (PulsarClientException e) {
                log.error("Failed to close consumer or pulsar client", e);
            }
        }));
        while (true) {
            Message<byte[]> message = consumer.receive();
            log.info("Received message messageId={} data={}", message.getMessageId(), new String(message.getData()));
            TimeUnit.SECONDS.sleep(1);
            log.info("Acknowledging message messageId={}", message.getMessageId());
            consumer.acknowledge(message);
        }
    }
}
