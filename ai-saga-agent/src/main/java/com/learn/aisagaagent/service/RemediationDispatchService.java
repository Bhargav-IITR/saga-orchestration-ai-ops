package com.learn.aisagaagent.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.sagacommons.dto.remediation.RemediationAction;
import com.learn.sagacommons.dto.remediation.RemediationCommand;
import com.learn.sagacommons.dto.remediation.RemediationEscalation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RemediationDispatchService {

    private static final Duration IDEMPOTENCY_TTL = Duration.ofDays(30);
    private static final String IDEMPOTENCY_PREFIX = "saga-remediation:action:";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.topic.inventory-remediation}")
    private String inventoryRemediationTopic;
    @Value("${spring.kafka.topic.payment-remediation}")
    private String paymentRemediationTopic;
    @Value("${spring.kafka.topic.remediation-escalation}")
    private String escalationTopic;

    public void dispatch(RemediationCommand command, List<RemediationAction> actions) {
        for (RemediationAction action : actions) {
            dispatchOne(command, action);
        }
    }

    public void escalate(RemediationCommand command, List<String> reasons) {
        RemediationEscalation escalation = RemediationEscalation.builder()
                .source("ai-saga-agent")
                .command(command)
                .reasons(reasons)
                .escalatedAt(LocalDateTime.now())
                .build();
        try {
            send(escalationTopic, command == null ? null : command.getOrderId(),
                    objectMapper.writeValueAsString(escalation));
        } catch (Exception ex) {
            log.error("[Remediation] Unable to publish human escalation: {}", ex.getMessage(), ex);
        }
    }

    private void dispatchOne(RemediationCommand command, RemediationAction action) {
        String idempotencyKey = IDEMPOTENCY_PREFIX + action.getActionId();
        final boolean reserved;
        try {
            reserved = reserve(idempotencyKey);
        } catch (RuntimeException ex) {
            log.error("[Remediation] Idempotency protection unavailable for action {}",
                    action.getActionId(), ex);
            escalate(command, List.of("Redis idempotency store is unavailable; action was not executed"));
            return;
        }
        if (!reserved) {
            log.info("[Remediation] Action {} was already dispatched; skipping duplicate", action.getActionId());
            return;
        }

        try {
            String topic = switch (action.getType()) {
                case RELEASE_INVENTORY -> inventoryRemediationTopic;
                case REFUND_PAYMENT -> paymentRemediationTopic;
                case ESCALATE -> throw new IllegalArgumentException("Escalation is not executable");
            };
            RemediationCommand singleActionCommand = copyWithAction(command, action);
            send(topic, command.getOrderId(), objectMapper.writeValueAsString(singleActionCommand));
            log.info("[Remediation] Dispatched actionId={} type={} topic={}",
                    action.getActionId(), action.getType(), topic);
        } catch (Exception ex) {
            try {
                redisTemplate.delete(idempotencyKey);
            } catch (RuntimeException cleanupError) {
                log.error("[Remediation] Could not clear failed reservation {}", idempotencyKey,
                        cleanupError);
            }
            log.error("[Remediation] Dispatch failed for action {}: {}", action.getActionId(), ex.getMessage());
            escalate(command, List.of("Failed to dispatch " + action.getType() + ": " + ex.getMessage()));
        }
    }

    private boolean reserve(String key) {
        try {
            return Boolean.TRUE.equals(redisTemplate.opsForValue()
                    .setIfAbsent(key, "DISPATCHED", IDEMPOTENCY_TTL));
        } catch (RuntimeException ex) {
            // Remediation is fail-closed: loss of idempotency protection requires a human.
            throw new IllegalStateException("Redis idempotency store is unavailable", ex);
        }
    }

    private void send(String topic, String key, String payload) throws Exception {
        kafkaTemplate.send(topic, key, payload).get(10, TimeUnit.SECONDS);
    }

    private RemediationCommand copyWithAction(RemediationCommand command, RemediationAction action)
            throws JsonProcessingException {
        RemediationCommand copy = objectMapper.readValue(
                objectMapper.writeValueAsString(command), RemediationCommand.class);
        copy.setActions(List.of(action));
        return copy;
    }
}
