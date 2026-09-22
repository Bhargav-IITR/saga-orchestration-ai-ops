package com.learn.orchestrated.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.orchestrated.orchestrator.service.producer.SagaOrchestratorProducer;
import com.learn.sagacommons.dto.Event;
import com.learn.sagacommons.dto.History;
import com.learn.sagacommons.enums.SagaStatusEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaTimeoutService {

    private static final String DEADLINES_KEY = "saga:active:deadlines";
    private static final String EVENT_PREFIX = "saga:active:event:";
    private static final String LOCK_PREFIX = "saga:active:timeout-lock:";

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final SagaOrchestratorProducer producer;

    @Value("${saga.timeout:PT2M}")
    private Duration timeout;

    @Value("${saga.timeout-batch-size:100}")
    private int batchSize;

    public void start(Event event) {
        if (event.getTransactionId() == null) return;
        try {
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            saveSnapshot(event);
            redis.opsForZSet().add(DEADLINES_KEY, event.getTransactionId(), deadline);
        } catch (RuntimeException ex) {
            log.error("Could not register saga timeout for transactionId={}",
                    event.getTransactionId(), ex);
        }
    }

    public void update(Event event) {
        if (event.getTransactionId() == null) return;
        try {
            Double deadline = redis.opsForZSet().score(DEADLINES_KEY, event.getTransactionId());
            if (deadline != null) saveSnapshot(event);
        } catch (RuntimeException ex) {
            log.error("Could not refresh saga timeout snapshot for transactionId={}",
                    event.getTransactionId(), ex);
        }
    }

    public void complete(String transactionId) {
        if (transactionId == null) return;
        try {
            redis.opsForZSet().remove(DEADLINES_KEY, transactionId);
            redis.delete(EVENT_PREFIX + transactionId);
            redis.delete(LOCK_PREFIX + transactionId);
        } catch (RuntimeException ex) {
            log.error("Could not clear saga timeout for transactionId={}", transactionId, ex);
        }
    }

    @Scheduled(fixedDelayString = "${saga.timeout-scan-delay-ms:5000}")
    public void publishExpiredSagas() {
        try {
            Set<String> expired = redis.opsForZSet().rangeByScore(
                    DEADLINES_KEY, 0, System.currentTimeMillis(), 0, batchSize);
            if (expired != null) expired.forEach(this::publishTimeout);
        } catch (RuntimeException ex) {
            log.error("Could not scan saga deadlines", ex);
        }
    }

    private void publishTimeout(String transactionId) {
        String lockKey = LOCK_PREFIX + transactionId;
        boolean locked = Boolean.TRUE.equals(redis.opsForValue()
                .setIfAbsent(lockKey, "processing", Duration.ofSeconds(30)));
        if (!locked) return;

        try {
            String json = redis.opsForValue().get(EVENT_PREFIX + transactionId);
            if (json == null) {
                complete(transactionId);
                return;
            }
            Event event = objectMapper.readValue(json, Event.class);
            event.setSource("ORCHESTRATOR");
            event.setStatus(SagaStatusEnum.TIMEOUT);
            event.addToHistory(History.builder()
                    .source("ORCHESTRATOR")
                    .status(SagaStatusEnum.TIMEOUT.toString())
                    .message("Saga deadline exceeded; terminating with failure")
                    .createdAt(LocalDateTime.now())
                    .build());
            producer.sendEvent("finish-fail", objectMapper.writeValueAsString(event));
            complete(transactionId);
            log.warn("Timed out saga transactionId={}", transactionId);
        } catch (Exception ex) {
            redis.delete(lockKey);
            log.error("Could not publish timeout for transactionId={}", transactionId, ex);
        }
    }

    private void saveSnapshot(Event event) {
        try {
            redis.opsForValue().set(
                    EVENT_PREFIX + event.getTransactionId(),
                    objectMapper.writeValueAsString(event),
                    timeout.plus(Duration.ofMinutes(5)));
        } catch (Exception ex) {
            throw new IllegalStateException("Could not persist saga timeout state", ex);
        }
    }
}
