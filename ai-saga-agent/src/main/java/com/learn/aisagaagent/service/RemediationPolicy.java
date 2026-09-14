package com.learn.aisagaagent.service;

import com.learn.sagacommons.dto.Event;
import com.learn.sagacommons.dto.remediation.RemediationAction;
import com.learn.sagacommons.dto.remediation.RemediationActionType;
import com.learn.sagacommons.dto.remediation.RemediationCommand;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.learn.sagacommons.enums.SagaStatusEnum.FAIL;
import static com.learn.sagacommons.dto.remediation.RemediationEvidence.hasFailedRollback;

@Component
public class RemediationPolicy {

    static final int MAX_ACTIONS = 3;
    static final BigDecimal MAX_AUTOMATED_REFUND = new BigDecimal("500.00");
    static final BigDecimal MAX_REFUND_PERCENTAGE = new BigDecimal("100.00");

    public PolicyResult validate(RemediationCommand command) {
        List<String> errors = new ArrayList<>();
        if (command == null) return PolicyResult.rejected("Missing remediation command");
        Event event = command.getSourceEvent();

        if (command.getSchemaVersion() != RemediationCommand.CURRENT_SCHEMA_VERSION) {
            errors.add("Unsupported remediation schema version");
        }
        if (isBlank(command.getRemediationId())) errors.add("Missing remediationId");
        if (isBlank(command.getOrderId()) || isBlank(command.getTransactionId())) {
            errors.add("orderId and transactionId are required");
        }
        if (event == null) {
            errors.add("Missing source event");
            return new PolicyResult(List.of(), errors);
        }
        if (!Objects.equals(command.getOrderId(), event.getOrderId())
                || !Objects.equals(command.getTransactionId(), event.getTransactionId())) {
            errors.add("Command identifiers do not match the immutable source event");
        }
        if (event.getStatus() != FAIL) errors.add("Only terminal FAIL events may be remediated");
        if (command.getRiskLevel() == null
                || !Set.of("LOW", "MEDIUM", "HIGH", "CRITICAL")
                .contains(command.getRiskLevel().toUpperCase(java.util.Locale.ROOT))) {
            errors.add("Risk level must be LOW, MEDIUM, HIGH, or CRITICAL");
        } else if ("CRITICAL".equalsIgnoreCase(command.getRiskLevel())) {
            errors.add("Critical-risk remediation requires a human operator");
        }

        List<RemediationAction> actions = command.getActions() == null
                ? List.of() : command.getActions();
        if (actions.isEmpty()) errors.add("No remediation action was supplied");
        if (actions.size() > MAX_ACTIONS) errors.add("Action count exceeds the hard limit of " + MAX_ACTIONS);

        Set<RemediationActionType> uniqueTypes = new HashSet<>();
        for (RemediationAction action : actions) {
            validateAction(action, event, uniqueTypes, errors);
        }

        return errors.isEmpty()
                ? new PolicyResult(List.copyOf(actions), List.of())
                : new PolicyResult(List.of(), List.copyOf(errors));
    }

    private void validateAction(RemediationAction action, Event event,
                                Set<RemediationActionType> uniqueTypes, List<String> errors) {
        if (action == null || action.getType() == null) {
            errors.add("Action type is required");
            return;
        }
        if (!uniqueTypes.add(action.getType())) {
            errors.add("Duplicate action type: " + action.getType());
        }
        if (isBlank(action.getActionId())) errors.add("Every action requires an actionId");
        if (isBlank(action.getReason()) || action.getReason().length() > 500) {
            errors.add("Action reason must contain 1 to 500 characters");
        }

        switch (action.getType()) {
            case RELEASE_INVENTORY -> {
                if (!hasFailedRollback(event, "inventory")) {
                    errors.add("Inventory release lacks evidence of a failed inventory rollback");
                }
            }
            case REFUND_PAYMENT -> validateRefund(action, event, errors);
            case ESCALATE -> errors.add("OperationsAgent requested human escalation");
        }
    }

    private void validateRefund(RemediationAction action, Event event, List<String> errors) {
        if (!hasFailedRollback(event, "payment")) {
            errors.add("Payment refund lacks evidence of a failed payment rollback");
        }
        BigDecimal percentage = action.getPercentage();
        if (percentage == null || percentage.signum() <= 0
                || percentage.compareTo(MAX_REFUND_PERCENTAGE) > 0) {
            errors.add("Refund percentage must be greater than 0 and at most 100");
            return;
        }
        if (event.getOrder() == null) {
            errors.add("Original order amount is required for a refund");
            return;
        }
        if (event.getOrder().getTotalAmount() <= 0) {
            errors.add("Original order amount must be greater than zero for a refund");
            return;
        }
        BigDecimal amount = BigDecimal.valueOf(event.getOrder().getTotalAmount())
                .multiply(percentage)
                .divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
        if (amount.compareTo(MAX_AUTOMATED_REFUND) > 0) {
            errors.add("Calculated refund exceeds the hard limit of R$ " + MAX_AUTOMATED_REFUND);
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    public record PolicyResult(List<RemediationAction> approvedActions, List<String> errors) {
        static PolicyResult rejected(String error) {
            return new PolicyResult(List.of(), List.of(error));
        }

        public boolean approved() {
            return errors.isEmpty();
        }
    }
}
