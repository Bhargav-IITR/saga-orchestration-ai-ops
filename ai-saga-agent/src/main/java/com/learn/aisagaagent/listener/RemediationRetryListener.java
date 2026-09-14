package com.learn.aisagaagent.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.aisagaagent.service.RemediationDispatchService;
import com.learn.aisagaagent.service.RemediationPolicy;
import com.learn.sagacommons.dto.remediation.RemediationCommand;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class RemediationRetryListener {

    private final ObjectMapper objectMapper;
    private final RemediationPolicy policy;
    private final RemediationDispatchService dispatcher;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.remediation-group-id}",
            topics = "${spring.kafka.topic.remediation-retry}")
    public void onRemediation(String payload) {
        RemediationCommand command;
        try {
            command = objectMapper.readValue(payload, RemediationCommand.class);
        } catch (JsonProcessingException ex) {
            log.error("[Remediation] Invalid retry payload; escalating without execution: {}", ex.getMessage());
            dispatcher.escalate(null, List.of("Malformed remediation JSON: " + ex.getOriginalMessage()));
            return;
        }

        RemediationPolicy.PolicyResult result = policy.validate(command);
        if (!result.approved()) {
            log.warn("[Remediation] Command {} rejected: {}", command.getRemediationId(), result.errors());
            dispatcher.escalate(command, result.errors());
            return;
        }

        dispatcher.dispatch(command, result.approvedActions());
    }
}
