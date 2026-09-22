package com.learn.aisagaagent.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.aisagaagent.dto.SagaPlan;
import com.learn.aisagaagent.service.agent.DataAnalystAgent;
import com.learn.aisagaagent.service.agent.SagaComposerAgent;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.mcp.McpToolProvider;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class SagaComposerService {

    private final EmbeddingModel embeddingModel;
    private final EmbeddingStore<TextSegment> embeddingStore;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final ChatModel primaryChatModel;
    private final McpToolProvider mcpToolProvider;

    private static final List<String> PROFILES = List.of(
            "new:high-value",
            "new:low-value",
            "vip:any",
            "returning:high-value",
            "returning:low-value",
            "default");

    @Value("${saga.composer.plan-ttl:PT30M}")
    private Duration planTtl;

    // ─── Scheduled job ────────────────────────────────────────────────────────
    @Scheduled(
            initialDelayString = "${saga.composer.initial-delay-ms:10000}",
            fixedDelayString = "${saga.composer.recompute-delay-ms:1800000}")
    public void recomputePlans() {
        log.info("[SagaComposer] Starting plan recomputation for {} profiles...", PROFILES.size());

        // Fetch system context once — shared across all profiles

        var dataAnalystAgent  = AiServices.builder(DataAnalystAgent.class)
                .chatModel(primaryChatModel)
                .toolProvider(mcpToolProvider)
                .maxSequentialToolsInvocations(3)
                .build();

        String metrics     = queryMetrics(dataAnalystAgent);
        String stockAlerts = queryStockAlerts(dataAnalystAgent);

        PROFILES.forEach(profile -> {
            try {
                String ragContext = findHistoricalPatterns(profile);
                String prompt    = buildCompositionPrompt(profile, metrics, stockAlerts, ragContext);

                var sagaComposerAgent = AiServices.builder(SagaComposerAgent.class)
                        .chatModel(primaryChatModel)
                        .maxSequentialToolsInvocations(3)
                        .build();

                String planJson  = sagaComposerAgent.compose(prompt);
                savePlan(profile, planJson);
            } catch (Exception e) {
                log.error("[SagaComposer] Failed to compute plan for profile '{}': {}",
                        profile, e.getMessage());
                // Continue with remaining profiles
            }
        });

        log.info("[SagaComposer] Recomputation complete. {} profiles saved to Redis.", PROFILES.size());
    }


    private String queryMetrics(DataAnalystAgent dataAnalystAgent) {
        try {

            return dataAnalystAgent.analyze(
                    "Get saga metrics for the last 15 orders — failure rate per service");
        } catch (Exception e) {
            log.warn("[SagaComposer] Metrics unavailable: {}", e.getMessage());
            return "Metrics unavailable.";
        }
    }

    private String queryStockAlerts(DataAnalystAgent dataAnalystAgent) {
        try {
            return dataAnalystAgent.analyze(
                    "Get low stock alerts — products with stock below 3 units");
        } catch (Exception e) {
            log.warn("[SagaComposer] Stock alerts unavailable: {}", e.getMessage());
            return "Stock alerts unavailable.";
        }
    }

    // ─── RAG: historical patterns per profile ─────────────────────────────────

    private String findHistoricalPatterns(String profileKey) {
        var embedding = embeddingModel.embed(
                "saga failure patterns for profile: " + profileKey).content();

        var results = embeddingStore.search(
                EmbeddingSearchRequest.builder()
                        .queryEmbedding(embedding)
                        .maxResults(5)
                        .minScore(0.70)
                        .build());

        if (results.matches().isEmpty())
            return "No historical patterns found for this profile yet.";

        return results.matches().stream()
                .map(m -> m.embedded().text())
                .collect(Collectors.joining("\n---\n"));
    }

    // ─── Prompt builder ───────────────────────────────────────────────────────

    private String buildCompositionPrompt(
            String profile, String metrics, String stockAlerts, String rag) {
        return """
                ORDER PROFILE: %s

                CURRENT SYSTEM METRICS (via MCP):
                %s

                CRITICAL STOCK ALERTS (via MCP):
                %s

                HISTORICAL FAILURE PATTERNS FOR THIS PROFILE (RAG):
                %s

                Compose the optimal saga plan for this profile.
                """.formatted(profile, metrics, stockAlerts, rag);
    }

    // ─── Redis persistence ────────────────────────────────────────────────────

    private void savePlan(String profile, String planJson) {
        try {
            String cleanJson = extractJson(planJson);
            var plan = objectMapper.readValue(cleanJson, SagaPlan.class);
            validatePlan(plan);
            plan.setProfileKey(profile);
            plan.setComputedAt(LocalDateTime.now());

            String key   = "saga-plan:" + profile;
            String value = objectMapper.writeValueAsString(plan);

            redis.opsForValue().set(key, value, planTtl);
            log.info("[SagaComposer] Plan saved: {} → steps={}", key, plan.getSteps());
        } catch (JsonProcessingException | IllegalArgumentException e) {
            log.error("[SagaComposer] Invalid JSON for profile '{}' — skipping. Raw: {}",
                    profile, planJson);
        }
    }

    private void validatePlan(SagaPlan plan) {
        Set<String> allowed = Set.of(
                "PRODUCT_VALIDATION", "FRAUD_VALIDATION", "PAYMENT", "INVENTORY");
        if (plan.getSteps() == null || plan.getSteps().isEmpty()) {
            throw new IllegalArgumentException("Plan must contain at least one step");
        }
        if (plan.getSteps().stream().distinct().count() != plan.getSteps().size()) {
            throw new IllegalArgumentException("Plan contains duplicate steps");
        }
        if (!allowed.containsAll(plan.getSteps())) {
            throw new IllegalArgumentException("Plan contains an unsupported step");
        }
    }

    private String extractJson(String text) {
        int start = text.indexOf('{');
        int end   = text.lastIndexOf('}');
        if (start == -1 || end == -1) {
            throw new IllegalArgumentException("No JSON found in response: " + text);
        }
        return text.substring(start, end + 1);
    }
}
