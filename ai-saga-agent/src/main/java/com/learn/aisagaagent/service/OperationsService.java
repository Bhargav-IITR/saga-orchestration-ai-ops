package com.learn.aisagaagent.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.aisagaagent.dto.DiagnosticDecision;
import com.learn.aisagaagent.model.SagaDiagnostic;
import com.learn.aisagaagent.repository.SagaDiagnosticRepository;
import com.learn.aisagaagent.service.agent.OperationsAgent;
import com.learn.sagacommons.dto.Event;
import com.learn.sagacommons.dto.remediation.RemediationAction;
import com.learn.sagacommons.dto.remediation.RemediationCommand;
import com.learn.sagacommons.enums.SagaStatusEnum;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingStore;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class OperationsService {

    private final EmbeddingModel embeddingModel;
    private final EmbeddingStore<TextSegment> embeddingStore;
    private final ChatModel primaryChatModel;
    private final SagaDiagnosticRepository diagnosticRepository;
    private final ObjectMapper objectMapper;
    private final SemanticDiagnosticCache diagnosticCache;
    private final DiagnosticDecisionParser decisionParser;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProfileClassifier profileClassifier;
    private OperationsAgent operationsAgent;

    @Value("${spring.kafka.topic.remediation-retry}")
    private String remediationRetryTopic;

    @PostConstruct
    void init() {
        this.operationsAgent = AiServices.builder(OperationsAgent.class)
                .chatModel(primaryChatModel)
                .build();
        log.info("[OperationsService] Virtual threads enabled: {}",
                Thread.currentThread().isVirtual());
    }

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${spring.kafka.topic.notify-ending}")
    public void onSagaEnded(String payload) {
        Event event = parseEvent(payload).orElse(null);
        if (event == null) {
            log.warn("[OperationsService] Failed to parse event payload - skipping");
            return;
        }

        String historyText = buildHistoryText(event);
        if (event.getStatus() == SagaStatusEnum.FAIL) {
            diagnose(event, historyText);
        } else {
            vectorize(event, historyText, null);
        }
    }

    private void diagnose(Event event, String historyText) {
        long startedAt = System.nanoTime();
        DiagnosticDecision decision;
        Embedding queryEmbedding = null;
        boolean traceCacheHit = false;
        boolean embeddingCacheHit = false;

        Optional<DiagnosticDecision> traceCached = cachedDecision(
                diagnosticCache.findByTrace(historyText));
        if (traceCached.isPresent()) {
            decision = traceCached.get();
            traceCacheHit = true;
        } else {
            try {
                queryEmbedding = embeddingModel.embed(historyText).content();
                Optional<DiagnosticDecision> embeddingCached = cachedDecision(
                        diagnosticCache.findByEmbedding(queryEmbedding));
                if (embeddingCached.isPresent()) {
                    decision = embeddingCached.get();
                    embeddingCacheHit = true;
                } else {
                    String ragContext = findSimilarIncidents(queryEmbedding);
                    String prompt = buildDiagnosticPrompt(event, historyText, ragContext);
                    decision = parseAgentResponse(operationsAgent.analyze(prompt));
                    diagnosticCache.put(historyText, queryEmbedding,
                            objectMapper.writeValueAsString(decision));
                }
            } catch (Exception ex) {
                log.error("[OperationsAgent] Diagnosis failed; forcing human escalation: {}",
                        ex.getMessage(), ex);
                decision = decisionParser.safeEscalation(
                        "Automated diagnosis failed: " + ex.getClass().getSimpleName());
            }
        }

        long elapsedMicros = (System.nanoTime() - startedAt) / 1_000;
        log.info("[OperationsAgent] Diagnosis resolved for orderId={} traceCacheHit={} "
                        + "embeddingCacheHit={} latencyMicros={}",
                event.getOrderId(), traceCacheHit, embeddingCacheHit, elapsedMicros);

        RemediationCommand command = toCommand(event, decision);
        String diagnosis = writeJson(command);
        try {
            kafkaTemplate.send(remediationRetryTopic, event.getOrderId(), diagnosis)
                    .get(10, TimeUnit.SECONDS);
            log.info("[OperationsAgent] Published remediation {} to {}",
                    command.getRemediationId(), remediationRetryTopic);
        } catch (Exception ex) {
            // Throwing lets the Kafka container retry the original notify-ending record.
            throw new IllegalStateException("Failed to publish remediation command", ex);
        }

        try {
            diagnosticRepository.save(SagaDiagnostic.builder()
                    .orderId(event.getOrderId())
                    .transactionId(event.getTransactionId())
                    .diagnosis(diagnosis)
                    .createdAt(LocalDateTime.now())
                    .build());
        } catch (RuntimeException ex) {
            // Compensation has already been durably accepted by Kafka. Keep the
            // operational path moving if the reporting table is temporarily down.
            log.error("[OperationsAgent] Remediation published but diagnostic persistence failed: {}",
                    ex.getMessage(), ex);
        }

        // Exact trace hits are duplicates and need not grow the vector store. New
        // incidents and embedding-cache hits retain the already calculated vector.
        if (!traceCacheHit && queryEmbedding != null) {
            vectorize(event, historyText, queryEmbedding);
        }
    }

    private void vectorize(Event event, String historyText, Embedding existingEmbedding) {
        String profileKey = classifyProfile(event);
        Metadata metadata = new Metadata()
                .put("orderId", event.getOrderId())
                .put("status", event.getStatus().toString())
                .put("profileKey", profileKey)
                .put("createdAt", LocalDateTime.now().toString());
        TextSegment segment = TextSegment.from(historyText, metadata);
        Embedding embedding = existingEmbedding != null
                ? existingEmbedding : embeddingModel.embed(segment).content();
        embeddingStore.add(embedding, segment);
    }

    private String classifyProfile(Event event) {
        if (event.getOrder() == null) return "default";
        return profileClassifier.classify(event.getOrder());
    }

    private String findSimilarIncidents(Embedding queryEmbedding) {
        var results = embeddingStore.search(
                EmbeddingSearchRequest.builder()
                        .queryEmbedding(queryEmbedding)
                        .maxResults(3)
                        .minScore(0.75)
                        .build());
        if (results.matches().isEmpty()) return "No similar incidents found in history.";

        return results.matches().stream()
                .map(match -> "--- Similar incident (score="
                        + String.format("%.2f", match.score()) + ") ---\n"
                        + match.embedded().text())
                .collect(Collectors.joining("\n\n"));
    }

    private String buildDiagnosticPrompt(Event event, String history, String rag) {
        double totalAmount = event.getOrder() != null ? event.getOrder().getTotalAmount() : 0.0;
        return """
                SAGA FAILED - DIAGNOSE
                OrderId: %s | TransactionId: %s
                Final status: %s
                Total amount: R$ %.2f

                SAGA HISTORY:
                %s

                SIMILAR INCIDENTS (RAG):
                %s
                """.formatted(event.getOrderId(), event.getTransactionId(), event.getStatus(),
                totalAmount, history, rag);
    }

    private String buildHistoryText(Event event) {
        if (event.getEventHistory() == null || event.getEventHistory().isEmpty()) {
            return "No saga history was supplied.";
        }
        return event.getEventHistory().stream()
                .filter(Objects::nonNull)
                .map(history -> history.getSource() + " [" + history.getStatus() + "]: "
                        + history.getMessage())
                .collect(Collectors.joining("\n"));
    }

    private Optional<DiagnosticDecision> cachedDecision(Optional<String> json) {
        if (json.isEmpty()) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(json.get(), DiagnosticDecision.class));
        } catch (JsonProcessingException ex) {
            log.warn("[SemanticDiagnosticCache] Ignoring malformed cached value: {}", ex.getMessage());
            return Optional.empty();
        }
    }

    private DiagnosticDecision parseAgentResponse(String response) {
        try {
            return decisionParser.parse(response);
        } catch (JsonProcessingException ex) {
            log.warn("[OperationsAgent] Invalid structured response; forcing escalation: {}", ex.getMessage());
            return decisionParser.safeEscalation("OperationsAgent returned invalid JSON");
        }
    }

    private RemediationCommand toCommand(Event event, DiagnosticDecision decision) {
        String identity = String.join(":",
                Optional.ofNullable(event.getEventId()).orElse(""),
                Optional.ofNullable(event.getOrderId()).orElse(""),
                Optional.ofNullable(event.getTransactionId()).orElse(""));
        String remediationId = UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
        List<RemediationAction> actions = new ArrayList<>();
        List<RemediationAction> proposed = decision.getActions() == null
                ? List.of() : decision.getActions();
        for (int i = 0; i < proposed.size(); i++) {
            RemediationAction source = proposed.get(i);
            if (source == null) continue;
            actions.add(RemediationAction.builder()
                    .actionId(remediationId + ":"
                            + (source.getType() == null ? "UNKNOWN" : source.getType().name())
                            + ":" + i)
                    .type(source.getType())
                    .percentage(source.getPercentage())
                    .reason(source.getReason())
                    .build());
        }

        double totalAmount = event.getOrder() == null ? 0.0 : event.getOrder().getTotalAmount();
        return RemediationCommand.builder()
                .schemaVersion(RemediationCommand.CURRENT_SCHEMA_VERSION)
                .remediationId(remediationId)
                .orderId(event.getOrderId())
                .transactionId(event.getTransactionId())
                .sourceEvent(event)
                .rootCause(decision.getRootCause())
                .affectedServices(decision.getAffectedServices())
                .financialImpact("R$ %.2f at risk. %s".formatted(totalAmount,
                        Optional.ofNullable(decision.getFinancialImpact()).orElse("")))
                .historicalPattern(decision.getHistoricalPattern())
                .recommendation(decision.getRecommendation())
                .riskLevel(decision.getRiskLevel())
                .actions(actions)
                .createdAt(LocalDateTime.now())
                .build();
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Could not serialize remediation command", ex);
        }
    }

    private Optional<Event> parseEvent(String json) {
        try {
            return Optional.ofNullable(objectMapper.readValue(json, Event.class));
        } catch (JsonProcessingException ex) {
            log.error("[OperationsService] Failed to deserialize event: {}", ex.getMessage());
            return Optional.empty();
        }
    }
}
