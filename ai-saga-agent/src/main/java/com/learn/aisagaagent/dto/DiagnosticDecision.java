package com.learn.aisagaagent.dto;

import com.learn.sagacommons.dto.remediation.RemediationAction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DiagnosticDecision {

    private String rootCause;
    @Builder.Default
    private List<String> affectedServices = new ArrayList<>();
    private String financialImpact;
    private String historicalPattern;
    private String recommendation;
    private String riskLevel;
    @Builder.Default
    private List<RemediationAction> actions = new ArrayList<>();
}
