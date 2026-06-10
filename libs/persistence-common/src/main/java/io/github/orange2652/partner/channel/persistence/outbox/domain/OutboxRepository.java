package io.github.orange2652.partner.channel.persistence.outbox.domain;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Outbox Port — Domain 이 의존하는 인터페이스 (Hexagonal).
 *
 * <p>구현체는 Infrastructure (JPA Adapter). Outbox 패턴의 도메인 측 경계로,
 * "DB 쓰기와 같은 Tx 안에 INSERT 된 row 가 별도 Relay 에 의해 Kafka 로 발행"
 * 되도록 한다 (Microservices Patterns Ch.3.6).</p>
 */
public interface OutboxRepository {

    /**
     * 새 outbox row INSERT. 호출자의 Tx 안에서 실행해야 dual-write 문제가 사라진다.
     *
     * @return DB 가 부여한 {@code id} 가 채워진 인스턴스
     */
    OutboxEvent save(OutboxEvent event);

    /**
     * {@code published_at IS NULL} 인 미발행 이벤트를 {@code id ASC} 순 최대 {@code limit} 건 조회.
     * Relay 가 호출.
     */
    List<OutboxEvent> findUnpublished(int limit);

    /**
     * Kafka 발행 완료 마킹 — {@code published_at} 갱신. Relay 가 send 성공 직후 호출.
     */
    void markPublished(Long id, LocalDateTime publishedAt);
}
