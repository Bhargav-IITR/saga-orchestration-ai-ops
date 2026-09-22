package com.learn.aisagaagent.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.aisagaagent.model.OperationalEscalation;
import com.learn.aisagaagent.repository.OperationalEscalationRepository;
import com.learn.sagacommons.dto.remediation.RemediationEscalation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class OperationalEscalationListener {

    private final ObjectMapper objectMapper;
    private final OperationalEscalationRepository repository;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.escalation-group-id}",
            topics = "${spring.kafka.topic.remediation-escalation}")
    public void onEscalation(String payload) throws Exception {
        RemediationEscalation escalation = objectMapper.readValue(
                payload, RemediationEscalation.class);
        var command = escalation.getCommand();
        repository.save(OperationalEscalation.builder()
                .source(escalation.getSource())
                .remediationId(command == null ? null : command.getRemediationId())
                .orderId(command == null ? null : command.getOrderId())
                .transactionId(command == null ? null : command.getTransactionId())
                .reasons(objectMapper.writeValueAsString(escalation.getReasons()))
                .payload(payload)
                .createdAt(escalation.getEscalatedAt() == null
                        ? LocalDateTime.now() : escalation.getEscalatedAt())
                .build());
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.dlt-group-id}",
            topics = "${spring.kafka.topic.remediation-dlt}")
    public void onDeadLetter(String payload) {
        try {
            repository.save(OperationalEscalation.builder()
                    .source("kafka-dlt")
                    .reasons("[\"Kafka listener retries exhausted\"]")
                    .payload(payload)
                    .createdAt(LocalDateTime.now())
                    .build());
            log.error("Persisted exhausted Kafka record from remediation DLT");
        } catch (RuntimeException ex) {
            // Never republish a failed DLT audit write back to the same DLT.
            log.error("Could not persist record already present in remediation DLT", ex);
        }
    }
}
