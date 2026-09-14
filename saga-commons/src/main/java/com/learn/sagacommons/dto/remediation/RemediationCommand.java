package com.learn.sagacommons.dto.remediation;

import com.learn.sagacommons.dto.Event;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RemediationCommand {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    private int schemaVersion;
    private String remediationId;
    private String orderId;
    private String transactionId;
    private Event sourceEvent;
    private String rootCause;
    private List<String> affectedServices;
    private String financialImpact;
    private String historicalPattern;
    private String recommendation;
    private String riskLevel;
    @Builder.Default
    private List<RemediationAction> actions = new ArrayList<>();
    private LocalDateTime createdAt;
}
