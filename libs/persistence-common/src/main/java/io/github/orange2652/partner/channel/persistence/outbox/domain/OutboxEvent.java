package io.github.orange2652.partner.channel.persistence.outbox.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * Outbox 도메인 객체 — 순수 POJO.
 * payload / headers 는 직렬화 책임을 호출자에 위임 (JSON 문자열).
 */
public record OutboxEvent(
        Long id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        String headers,
        Instant createdAt,
        Instant publishedAt
) {
    public OutboxEvent {
        Objects.requireNonNull(aggregateType, "aggregateType");
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(createdAt, "createdAt");
    }

    public static OutboxEvent newEvent(String aggregateType,
                                       String aggregateId,
                                       String eventType,
                                       String payload,
                                       String headers) {
        return new OutboxEvent(null, aggregateType, aggregateId, eventType,
                payload, headers, Instant.now(), null);
    }

    public OutboxEvent markPublished(Instant at) {
        return new OutboxEvent(id, aggregateType, aggregateId, eventType,
                payload, headers, createdAt, at);
    }

    public boolean isPublished() {
        return publishedAt != null;
    }
}
