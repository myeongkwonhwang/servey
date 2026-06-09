package io.github.orange2652.partner.channel.persistence.idempotency.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * Consumer 멱등성 — 동일 (consumer_name, event_id) 두 번 처리 방지.
 * Microservices Patterns Ch.3.3.2.
 */
public record ProcessedEvent(
        String consumerName,
        String eventId,
        Instant processedAt
) {
    public ProcessedEvent {
        Objects.requireNonNull(consumerName, "consumerName");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(processedAt, "processedAt");
    }

    public static ProcessedEvent newRecord(String consumerName, String eventId) {
        return new ProcessedEvent(consumerName, eventId, Instant.now());
    }
}
