package com.learn.orchestrated.payment.service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.orchestrated.payment.service.service.PaymentService;
import com.learn.sagacommons.dto.remediation.RemediationAction;
import com.learn.sagacommons.dto.remediation.RemediationActionType;
import com.learn.sagacommons.dto.remediation.RemediationCommand;
import com.learn.sagacommons.dto.remediation.RemediationEscalation;
import com.learn.sagacommons.dto.remediation.RemediationEvidence;
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
public class PaymentRemediationConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic.remediation-escalation}")
    private String escalationTopic;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.remediation-group-id}",
            topics = "${spring.kafka.topic.payment-remediation}")
    public void consume(String payload) {
        RemediationCommand command = null;
        try {
            command = objectMapper.readValue(payload, RemediationCommand.class);
            apply(command);
        } catch (Exception ex) {
            log.error("Payment remediation failed; escalating: {}", ex.getMessage(), ex);
            escalate(command, ex.getMessage());
        }
    }

    private void apply(RemediationCommand command) {
        if (command.getActions() == null || command.getActions().size() != 1) {
            throw new IllegalArgumentException("Payment remediation requires exactly one action");
        }
        RemediationAction action = command.getActions().getFirst();
        if (action.getType() != RemediationActionType.REFUND_PAYMENT) {
            throw new IllegalArgumentException("Unsupported payment remediation action");
        }
        if (command.getSourceEvent() == null
                || command.getSourceEvent().getStatus() != com.learn.sagacommons.enums.SagaStatusEnum.FAIL
                || !Objects.equals(command.getOrderId(), command.getSourceEvent().getOrderId())
                || !Objects.equals(command.getTransactionId(), command.getSourceEvent().getTransactionId())) {
            throw new IllegalArgumentException("Remediation identifiers do not match source event");
        }
        if (!RemediationEvidence.hasFailedRollback(command.getSourceEvent(), "payment")) {
            throw new IllegalArgumentException("Payment remediation lacks failed rollback evidence");
        }

        paymentService.applyAutomatedRefund(command.getSourceEvent(), action.getPercentage());
        log.info("Applied payment remediation actionId={} orderId={}",
                action.getActionId(), command.getOrderId());
    }

    private void escalate(RemediationCommand command, String reason) {
        try {
            RemediationEscalation escalation = RemediationEscalation.builder()
                    .source("payment-service")
                    .command(command)
                    .reasons(List.of(reason == null ? "Unknown payment remediation error" : reason))
                    .escalatedAt(LocalDateTime.now())
                    .build();
            kafkaTemplate.send(escalationTopic,
                    command == null ? null : command.getOrderId(),
                    objectMapper.writeValueAsString(escalation));
        } catch (Exception publishError) {
            log.error("Could not publish payment remediation escalation", publishError);
        }
    }
}
