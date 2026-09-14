package com.learn.aisagaagent.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.sagacommons.dto.remediation.RemediationActionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DiagnosticDecisionParserTest {

    private final DiagnosticDecisionParser parser = new DiagnosticDecisionParser(new ObjectMapper());

    @Test
    void acceptsJsonWrappedInACommonModelMarkdownFence() throws Exception {
        String response = """
                ```json
                {
                  "rootCause":"payment rollback failed",
                  "affectedServices":["PAYMENT_SERVICE"],
                  "financialImpact":"MEDIUM",
                  "historicalPattern":"none",
                  "recommendation":"retry",
                  "riskLevel":"MEDIUM",
                  "actions":[{"type":"REFUND_PAYMENT","percentage":25,"reason":"rollback failed"}]
                }
                ```
                """;

        var decision = parser.parse(response);

        assertEquals(RemediationActionType.REFUND_PAYMENT, decision.getActions().getFirst().getType());
        assertEquals("payment rollback failed", decision.getRootCause());
    }
}
