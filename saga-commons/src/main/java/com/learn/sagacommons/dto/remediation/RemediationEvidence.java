package com.learn.sagacommons.dto.remediation;

import com.learn.sagacommons.dto.Event;
import com.learn.sagacommons.dto.History;

import java.util.Locale;
import java.util.Objects;

public final class RemediationEvidence {

    private RemediationEvidence() { }

    public static boolean hasFailedRollback(Event event, String service) {
        if (event == null || event.getEventHistory() == null || service == null) return false;
        String normalizedService = service.toLowerCase(Locale.ROOT);
        return event.getEventHistory().stream()
                .filter(Objects::nonNull)
                .map(History::getMessage)
                .filter(Objects::nonNull)
                .map(message -> message.toLowerCase(Locale.ROOT))
                .anyMatch(message -> message.contains("rollback not executed")
                        && message.contains(normalizedService));
    }
}
