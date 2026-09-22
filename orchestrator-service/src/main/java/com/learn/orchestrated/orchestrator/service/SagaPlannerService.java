package com.learn.orchestrated.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.orchestrated.orchestrator.service.dto.SagaPlan;
import com.learn.sagacommons.dto.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaPlannerService {

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    private static final String KEY_PREFIX = "saga-plan:";
    private static final String DEFAULT_FIRST_TOPIC = "product-validation-success";

    public String getFirstTopicForOrder(Order order) {
        String profileKey = resolveProfile(order);
        return getFirstTopicForProfile(profileKey);
    }

    private String getFirstTopicForProfile(String profileKey) {
        try {
            String json = redis.opsForValue().get(KEY_PREFIX + profileKey);

            if (json == null) {
                log.info("[SagaPlanner] No plan found for profile '{}' — using default", profileKey);
                return DEFAULT_FIRST_TOPIC;
            }

            var node = objectMapper.readTree(json);
            var steps = node.get("steps");

            if (steps == null || steps.isEmpty()) {
                log.warn("[SagaPlanner] Empty steps for profile '{}' — using default", profileKey);
                return DEFAULT_FIRST_TOPIC;
            }

            String firstStep = steps.get(0).asText();
            String firstTopic = resolveTopicFromStep(firstStep);
            log.info("[SagaPlanner] Profile '{}' → firstStep='{}' → topic='{}'",
                    profileKey, firstStep, firstTopic);
            return firstTopic;

        } catch (Exception e) {
            log.warn("[SagaPlanner] Failed to read plan from Redis — using default: {}", e.getMessage());
            return DEFAULT_FIRST_TOPIC;
        }
    }

    // NOVO — próximo tópico baseado no passo actual
    public String getNextTopicForOrder(Order order, String completedStep) {
        try {
            String profile     = resolveProfile(order);
            SagaPlan plan      = getPlan(profile);
            List<String> steps = plan.getSteps();

            // Normaliza "PRODUCT_VALIDATION_SERVICE" → "PRODUCT_VALIDATION"
            String normalizedStep = normalizeSource(completedStep);

            log.info("[SagaPlanner] source='{}' | normalized='{}' | steps={}",
                    completedStep, normalizedStep, steps);

            int currentIndex = steps.indexOf(normalizedStep);

            if (currentIndex == -1) {
                log.warn("[SagaPlanner] Step '{}' not found in plan — fallback",
                        normalizedStep);
                return null;
            }

            if (currentIndex == steps.size() - 1) {
                return "FINISH_SUCCESS";
            }

            String nextStep  = steps.get(currentIndex + 1);
            String nextTopic = plan.stepToTopic(nextStep);

            log.info("[SagaPlanner] {} → {} | topic: {}",
                    normalizedStep, nextStep, nextTopic);

            return nextTopic;
        } catch (Exception e) {
            log.warn("[SagaPlanner] Error getting next topic — fallback: {}", e.getMessage());
            return null; // null = usa SAGA_HANDLER
        }
    }

    private String normalizeSource(String source) {
        return switch (source) {
            case "PRODUCT_VALIDATION_SERVICE" -> "PRODUCT_VALIDATION";
            case "FRAUD_VALIDATION_SERVICE"   -> "FRAUD_VALIDATION";
            case "PAYMENT_SERVICE"            -> "PAYMENT";
            case "INVENTORY_SERVICE"          -> "INVENTORY";
            case "ORCHESTRATOR"               -> "ORCHESTRATOR";
            default                           -> source;
        };
    }

    // NOVO — rollback do passo anterior no plano
    public String getPreviousRollbackTopic(Order order, String failedStep) {
        RollbackDecision decision = getRollbackDecision(order, failedStep);
        return decision.type() == RollbackDecisionType.ROUTE ? decision.topic() : null;
    }

    public RollbackDecision getCurrentRollbackDecision(Order order, String currentStep) {
        try {
            SagaPlan plan = getPlan(resolveProfile(order));
            String normalizedStep = normalizeSource(currentStep);
            if (!plan.getSteps().contains(normalizedStep)) {
                return RollbackDecision.fallback();
            }
            return RollbackDecision.route(plan.rollbackTopic(normalizedStep));
        } catch (Exception ex) {
            log.warn("[SagaPlanner] Error getting current rollback topic: {}", ex.getMessage());
            return RollbackDecision.fallback();
        }
    }

    public RollbackDecision getRollbackDecision(Order order, String failedStep) {
        try {
            String profile = resolveProfile(order);
            SagaPlan plan = getPlan(profile);
            List<String> steps = plan.getSteps();

            String normalizedStep = normalizeSource(failedStep);

            log.info("[SagaPlanner] rollback | source='{}' | normalized='{}' | steps={}",
                    failedStep, normalizedStep, steps);

            int currentIndex = steps.indexOf(normalizedStep);

            log.info("[SagaPlanner] indexOf='{}' | currentIndex={}",
                    normalizedStep, currentIndex);

            if (currentIndex == -1) {
                log.warn("[SagaPlanner] Step not found in plan — fallback SAGA_HANDLER");
                return RollbackDecision.fallback();
            }

            if (currentIndex == 0) {
                log.info("[SagaPlanner] First step reached — finishSagaFail");
                return RollbackDecision.complete();
            }

            String previousStep   = steps.get(currentIndex - 1);
            String previousTopic  = plan.rollbackTopic(previousStep);

            log.info("[SagaPlanner] rollback chain: {} → {} | topic: {}",
                    normalizedStep, previousStep, previousTopic);

            return RollbackDecision.route(previousTopic);

        } catch (Exception e) {
            log.warn("[SagaPlanner] Error getting rollback topic: {}", e.getMessage());
            return RollbackDecision.fallback();
        }
    }

    private SagaPlan getPlan(String profile) throws Exception {
        String key = "saga-plan:" + profile;
        String json = redis.opsForValue().get(key);
        if (json == null) throw new RuntimeException("No plan for profile: " + profile);
        SagaPlan plan = objectMapper.readValue(json, SagaPlan.class);
        validatePlan(plan);
        return plan;
    }

    private void validatePlan(SagaPlan plan) {
        if (plan.getSteps() == null || plan.getSteps().isEmpty()) {
            throw new IllegalArgumentException("Saga plan must contain at least one step");
        }
        if (plan.getSteps().stream().distinct().count() != plan.getSteps().size()) {
            throw new IllegalArgumentException("Saga plan contains duplicate steps");
        }
        for (String step : plan.getSteps()) {
            plan.stepToTopic(step);
            plan.rollbackTopic(step);
        }
    }

    private String resolveProfile(Order order) {
        if (order == null) return "default";
        String clientType = Optional.ofNullable(order.getClientType()).orElse("default");
        Double totalAmount = Optional.ofNullable(order.getTotalAmount()).orElse(0.0);

        return switch (clientType) {
            case "new" -> totalAmount >= 200 ? "new:high-value" : "new:low-value";
            case "vip" -> "vip:any";
            case "returning" -> totalAmount >= 200 ? "returning:high-value" : "returning:low-value";
            default -> "default";
        };
    }

    private String resolveTopicFromStep(String step) {
        return switch (step) {
            case "PRODUCT_VALIDATION" -> "product-validation-success";
            case "PAYMENT" -> "payment-success";
            case "FRAUD_VALIDATION" -> "fraud-validation-success";
            case "INVENTORY" -> "inventory-success";
            default -> {
                log.warn("[SagaPlanner] Unknown step '{}' — using default", step);
                yield DEFAULT_FIRST_TOPIC;
            }
        };
    }

    public enum RollbackDecisionType { ROUTE, COMPLETE, FALLBACK }

    public record RollbackDecision(RollbackDecisionType type, String topic) {
        static RollbackDecision route(String topic) {
            return new RollbackDecision(RollbackDecisionType.ROUTE, topic);
        }

        static RollbackDecision complete() {
            return new RollbackDecision(RollbackDecisionType.COMPLETE, null);
        }

        static RollbackDecision fallback() {
            return new RollbackDecision(RollbackDecisionType.FALLBACK, null);
        }
    }
}
