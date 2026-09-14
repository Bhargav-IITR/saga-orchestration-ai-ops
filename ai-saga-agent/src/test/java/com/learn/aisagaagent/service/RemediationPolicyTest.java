package com.learn.aisagaagent.service;

import com.learn.sagacommons.dto.Event;
import com.learn.sagacommons.dto.History;
import com.learn.sagacommons.dto.Order;
import com.learn.sagacommons.dto.remediation.RemediationAction;
import com.learn.sagacommons.dto.remediation.RemediationActionType;
import com.learn.sagacommons.dto.remediation.RemediationCommand;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static com.learn.sagacommons.enums.SagaStatusEnum.FAIL;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemediationPolicyTest {

    private final RemediationPolicy policy = new RemediationPolicy();

    @Test
    void approvesBoundedRefundWhenFailedRollbackIsInImmutableHistory() {
        RemediationCommand command = command(
                "Rollback not executed for payment: gateway unavailable",
                action(RemediationActionType.REFUND_PAYMENT, "50"), 200.0);

        assertTrue(policy.validate(command).approved());
    }

    @Test
    void rejectsActionThatHasNoFailedRollbackEvidence() {
        RemediationCommand command = command(
                "Payment realized successfully!",
                action(RemediationActionType.REFUND_PAYMENT, "50"), 200.0);

        RemediationPolicy.PolicyResult result = policy.validate(command);

        assertFalse(result.approved());
        assertTrue(result.errors().stream().anyMatch(error -> error.contains("lacks evidence")));
    }

    @Test
    void rejectsRefundAboveHardAmountLimit() {
        RemediationCommand command = command(
                "Rollback not executed for payment: gateway unavailable",
                action(RemediationActionType.REFUND_PAYMENT, "100"), 501.0);

        RemediationPolicy.PolicyResult result = policy.validate(command);

        assertFalse(result.approved());
        assertTrue(result.errors().stream().anyMatch(error -> error.contains("hard limit")));
    }

    @Test
    void approvesInventoryReleaseWhenItsRollbackFailed() {
        RemediationCommand command = command(
                "Rollback not executed for inventory: database timeout",
                action(RemediationActionType.RELEASE_INVENTORY, null), 200.0);

        assertTrue(policy.validate(command).approved());
    }

    @Test
    void rejectsAutomationForCriticalRisk() {
        RemediationCommand command = command(
                "Rollback not executed for inventory: database timeout",
                action(RemediationActionType.RELEASE_INVENTORY, null), 200.0);
        command.setRiskLevel("CRITICAL");

        assertFalse(policy.validate(command).approved());
    }

    private RemediationCommand command(String historyMessage, RemediationAction action, double amount) {
        Order order = new Order();
        order.setOrderId("order-1");
        order.setTotalAmount(amount);
        Event event = Event.builder()
                .eventId("event-1")
                .orderId("order-1")
                .transactionId("tx-1")
                .order(order)
                .status(FAIL)
                .eventHistory(List.of(History.builder().message(historyMessage).build()))
                .build();
        return RemediationCommand.builder()
                .schemaVersion(RemediationCommand.CURRENT_SCHEMA_VERSION)
                .remediationId("remediation-1")
                .orderId("order-1")
                .transactionId("tx-1")
                .sourceEvent(event)
                .riskLevel("MEDIUM")
                .actions(List.of(action))
                .build();
    }

    private RemediationAction action(RemediationActionType type, String percentage) {
        return RemediationAction.builder()
                .actionId("action-1")
                .type(type)
                .percentage(percentage == null ? null : new BigDecimal(percentage))
                .reason("Retry the failed compensation")
                .build();
    }
}
