package com.learn.aisagaagent.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SemanticDiagnosticCacheTest {

    private final SemanticDiagnosticCache cache = new SemanticDiagnosticCache(null);

    @Test
    void normalizesRuntimeIdentifiersBeforeHashing() {
        String first = "2026-09-14T10:20:30Z failure tx "
                + "123e4567-e89b-12d3-a456-426614174000 at 0xA12F request 12345678";
        String second = "2026-09-15T11:21:31Z failure tx "
                + "223e4567-e89b-12d3-a456-426614174999 at 0xB34A request 87654321";

        assertEquals(cache.traceKey(first), cache.traceKey(second));
    }
}
