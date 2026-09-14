package com.learn.orchestrated.inventory.service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.orchestrated.inventory.service.service.InventoryService;
import com.learn.sagacommons.dto.remediation.RemediationAction;
import com.learn.sagacommons.dto.remediation.RemediationActionType;
import com.learn.sagacommons.dto.remediation.RemediationCommand;
import com.learn.sagacommons.dto.remediation.RemediationEscalation;
import com.learn.sagacommons.dto.remediation.RemediationEvidence;
import com.learn.sagacommons.enums.SagaStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryRemediationConsumer {

    private final ObjectMapper objectMapper;
    private final InventoryService inventoryService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic.remediation-escalation}")
    private String escalationTopic;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.remediation-group-id}",
            topics = "${spring.kafka.topic.inventory-remediation}")
    public void consume(String payload) {
        RemediationCommand command = null;
        try {
            command = objectMapper.readValue(payload, RemediationCommand.class);
            apply(command);
        } catch (Exception ex) {
            log.error("Inventory remediation failed; escalating: {}", ex.getMessage(), ex);
            escalate(command, ex.getMessage());
        }
    }

    private void apply(RemediationCommand command) {
        if (command.getActions() == null || command.getActions().size() != 1) {
            throw new IllegalArgumentException("Inventory remediation requires exactly one action");
        }
        RemediationAction action = command.getActions().getFirst();
        if (action.getType() != RemediationActionType.RELEASE_INVENTORY) {
            throw new IllegalArgumentException("Unsupported inventory remediation action");
        }
        if (command.getSourceEvent() == null
                || command.getSourceEvent().getStatus() != SagaStatusEnum.FAIL
                || !Objects.equals(command.getOrderId(), command.getSourceEvent().getOrderId())
                || !Objects.equals(command.getTransactionId(), command.getSourceEvent().getTransactionId())) {
            throw new IllegalArgumentException("Remediation identifiers do not match source event");
        }
        if (!RemediationEvidence.hasFailedRollback(command.getSourceEvent(), "inventory")) {
            throw new IllegalArgumentException("Inventory remediation lacks failed rollback evidence");
        }

        inventoryService.releaseInventoryForRemediation(command.getSourceEvent());
        log.info("Applied inventory remediation actionId={} orderId={}",
                action.getActionId(), command.getOrderId());
    }

    private void escalate(RemediationCommand command, String reason) {
        try {
            RemediationEscalation escalation = RemediationEscalation.builder()
                    .source("inventory-service")
                    .command(command)
                    .reasons(List.of(reason == null ? "Unknown inventory remediation error" : reason))
                    .escalatedAt(LocalDateTime.now())
                    .build();
            kafkaTemplate.send(escalationTopic,
                    command == null ? null : command.getOrderId(),
                    objectMapper.writeValueAsString(escalation));
        } catch (Exception publishError) {
            log.error("Could not publish inventory remediation escalation", publishError);
        }
    }
}
