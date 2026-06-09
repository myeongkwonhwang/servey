package io.github.orange2652.partner.channel.persistence.outbox.domain;

import java.time.Instant;
import java.util.List;

/**
 * Outbox Port — Domain 이 의존하는 인터페이스 (Hexagonal).
 * 구현체는 Infrastructure (JPA Adapter).
 */
public interface OutboxRepository {

    OutboxEvent save(OutboxEvent event);

    List<OutboxEvent> findUnpublished(int limit);

    void markPublished(Long id, Instant publishedAt);
}
