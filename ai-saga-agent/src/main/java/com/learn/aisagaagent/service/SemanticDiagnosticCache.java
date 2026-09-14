package com.learn.aisagaagent.service;

import dev.langchain4j.data.embedding.Embedding;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class SemanticDiagnosticCache {

    private static final String PREFIX = "saga-diagnostic:v1:";
    private static final Pattern ISO_TIMESTAMP = Pattern.compile(
            "\\b\\d{4}-\\d{2}-\\d{2}[tT ][0-9:.+\\-zZ]+\\b");
    private static final Pattern UUID = Pattern.compile(
            "\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b");
    private static final Pattern HEX_ADDRESS = Pattern.compile("0x[0-9a-fA-F]+");
    private static final Pattern LONG_NUMBER = Pattern.compile("\\b\\d{6,}\\b");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${operations.cache.ttl:PT24H}")
    private Duration ttl;

    public Optional<String> findByTrace(String history) {
        return get(traceKey(history));
    }

    public Optional<String> findByEmbedding(Embedding embedding) {
        return get(embeddingKey(embedding));
    }

    public void put(String history, Embedding embedding, String diagnosticJson) {
        put(traceKey(history), diagnosticJson);
        put(embeddingKey(embedding), diagnosticJson);
    }

    String traceKey(String history) {
        return PREFIX + "trace:" + sha256(normalize(history).getBytes(StandardCharsets.UTF_8));
    }

    String embeddingKey(Embedding embedding) {
        float[] vector = embedding.vector();
        ByteBuffer bytes = ByteBuffer.allocate(vector.length * Float.BYTES);
        for (float value : vector) bytes.putFloat(value);
        return PREFIX + "embedding:" + sha256(bytes.array());
    }

    String normalize(String input) {
        String normalized = input == null ? "" : input.toLowerCase(Locale.ROOT);
        normalized = ISO_TIMESTAMP.matcher(normalized).replaceAll("<timestamp>");
        normalized = UUID.matcher(normalized).replaceAll("<uuid>");
        normalized = HEX_ADDRESS.matcher(normalized).replaceAll("<address>");
        normalized = LONG_NUMBER.matcher(normalized).replaceAll("<number>");
        return WHITESPACE.matcher(normalized).replaceAll(" ").trim();
    }

    private Optional<String> get(String key) {
        try {
            return Optional.ofNullable(redisTemplate.opsForValue().get(key));
        } catch (RuntimeException ex) {
            log.warn("[SemanticDiagnosticCache] Redis read failed; continuing without cache: {}",
                    ex.getMessage());
            return Optional.empty();
        }
    }

    private void put(String key, String value) {
        try {
            redisTemplate.opsForValue().set(key, value, ttl);
        } catch (RuntimeException ex) {
            log.warn("[SemanticDiagnosticCache] Redis write failed; diagnosis was not cached: {}",
                    ex.getMessage());
        }
    }

    private String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is required by the JVM", ex);
        }
    }
}
