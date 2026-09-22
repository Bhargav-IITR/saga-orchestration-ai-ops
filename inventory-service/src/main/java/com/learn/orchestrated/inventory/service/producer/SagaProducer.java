package com.learn.orchestrated.inventory.service.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class SagaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic.orchestrator}")
    private String orchestratorTopic;

    public void sendEvent(String playload) {
        try {
            kafkaTemplate.send(orchestratorTopic, playload).get(10, TimeUnit.SECONDS);
            log.info("Success to send data to topic {} with data {}", orchestratorTopic, playload);

        } catch (Exception e) {
            log.error("Error trying to send data to topic {} with data {}", orchestratorTopic, playload, e);
            throw new IllegalStateException("Could not publish inventory result", e);
        }
    }
}
