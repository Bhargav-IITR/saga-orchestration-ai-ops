package com.learn.sagacommons.dto.remediation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RemediationAction {

    private String actionId;
    private RemediationActionType type;

    /**
     * Target percentage of the original payment to refund. It is ignored for
     * non-payment actions.
     */
    private BigDecimal percentage;
    private String reason;
}
