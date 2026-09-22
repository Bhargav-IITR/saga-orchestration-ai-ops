package com.learn.orchestrated.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.sagacommons.dto.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SagaPlannerServiceTest {

    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private SagaPlannerService planner;
    private Order order;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        planner = new SagaPlannerService(redis, new ObjectMapper());
        order = new Order();
        order.setClientType("returning");
        order.setTotalAmount(250.0);
    }

    @Test
    void routesFraudFirstForReturningHighValuePlan() {
        when(values.get("saga-plan:returning:high-value"))
                .thenReturn("{\"steps\":[\"FRAUD_VALIDATION\",\"PAYMENT\"]}");

        assertEquals("fraud-validation-success", planner.getFirstTopicForOrder(order));
    }

    @Test
    void distinguishesCompletedRollbackFromMissingPlan() {
        when(values.get("saga-plan:returning:high-value"))
                .thenReturn("{\"steps\":[\"PAYMENT\",\"INVENTORY\"]}");

        var complete = planner.getRollbackDecision(order, "PAYMENT_SERVICE");
        assertEquals(SagaPlannerService.RollbackDecisionType.COMPLETE, complete.type());

        when(values.get("saga-plan:returning:high-value")).thenReturn(null);
        var fallback = planner.getRollbackDecision(order, "PAYMENT_SERVICE");
        assertEquals(SagaPlannerService.RollbackDecisionType.FALLBACK, fallback.type());
    }

    @Test
    void rollsBackCurrentStepBeforeWalkingToPreviousSteps() {
        when(values.get("saga-plan:returning:high-value"))
                .thenReturn("{\"steps\":[\"PAYMENT\",\"INVENTORY\"]}");

        var decision = planner.getCurrentRollbackDecision(order, "INVENTORY_SERVICE");

        assertEquals(SagaPlannerService.RollbackDecisionType.ROUTE, decision.type());
        assertEquals("inventory-fail", decision.topic());
    }
}
