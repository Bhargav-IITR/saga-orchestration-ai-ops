package com.learn.aisagaagent.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.aisagaagent.dto.DiagnosticDecision;
import com.learn.sagacommons.dto.remediation.RemediationAction;
import com.learn.sagacommons.dto.remediation.RemediationActionType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DiagnosticDecisionParser {

    private final ObjectMapper objectMapper;

    public DiagnosticDecision parse(String response) throws JsonProcessingException {
        String json = stripMarkdownFence(response);
        DiagnosticDecision decision = objectMapper.readValue(json, DiagnosticDecision.class);
        if (decision.getRootCause() == null || decision.getRootCause().isBlank()) {
            throw new JsonProcessingException("OperationsAgent response is missing rootCause") { };
        }
        return decision;
    }

    public DiagnosticDecision safeEscalation(String reason) {
        return DiagnosticDecision.builder()
                .rootCause(reason)
                .affectedServices(List.of("UNKNOWN"))
                .financialImpact("Requires human assessment")
                .historicalPattern("Unavailable because the diagnostic response was invalid")
                .recommendation("Escalate to an operator; do not execute an automated action")
                .riskLevel("HIGH")
                .actions(List.of(RemediationAction.builder()
                        .type(RemediationActionType.ESCALATE)
                        .reason(reason)
                        .build()))
                .build();
    }

    private String stripMarkdownFence(String value) {
        if (value == null) return "";
        String trimmed = value.trim();
        if (!trimmed.startsWith("```")) return trimmed;

        int firstLineEnd = trimmed.indexOf('\n');
        int closingFence = trimmed.lastIndexOf("```");
        if (firstLineEnd < 0 || closingFence <= firstLineEnd) return trimmed;
        return trimmed.substring(firstLineEnd + 1, closingFence).trim();
    }
}
