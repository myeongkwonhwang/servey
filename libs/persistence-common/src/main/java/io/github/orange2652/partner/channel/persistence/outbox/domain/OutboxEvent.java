package io.github.orange2652.partner.channel.persistence.outbox.domain;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Outbox 도메인 객체 — 순수 POJO.
 *
 * <p>{@code payload} / {@code headers} 는 직렬화 책임을 호출자에 위임 (JSON 문자열).
 * 도메인 record 가 직렬화 형식을 알 필요가 없도록 설계.</p>
 *
 * <p>{@code aggregateId} 는 Kafka 메시지 key 로 사용되어 같은 aggregate 의 이벤트가
 * 같은 partition 에 가도록 순서 보장에 기여.</p>
 *
 * @param id            DB PK (신규는 null)
 * @param aggregateType 집계 종류 (예: {@code "ORDER"}) — 토픽 라우팅에 사용
 * @param aggregateId   집계 식별자 — Kafka key
 * @param eventType     이벤트 종류 (예: {@code "OrderReceived"})
 * @param payload       이벤트 본문 (JSON)
 * @param headers       메타데이터 (JSON, optional)
 * @param createdAt     INSERT 시각
 * @param publishedAt   Kafka 발행 완료 시각 (미발행은 null)
 */
public record OutboxEvent(
        Long id,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        String headers,
        LocalDateTime createdAt,
        LocalDateTime publishedAt
) {
    public OutboxEvent {
        Objects.requireNonNull(aggregateType, "aggregateType");
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(createdAt, "createdAt");
    }

    /**
     * 미발행 신규 이벤트 — {@code id=null}, {@code publishedAt=null}, {@code createdAt=now}.
     */
    public static OutboxEvent newEvent(String aggregateType,
                                       String aggregateId,
                                       String eventType,
                                       String payload,
                                       String headers) {
        return new OutboxEvent(null, aggregateType, aggregateId, eventType,
                payload, headers, LocalDateTime.now(), null);
    }

    /**
     * Kafka 발행 완료 마킹 — {@code publishedAt} 만 갱신한 새 인스턴스 반환 (불변).
     */
    public OutboxEvent markPublished(LocalDateTime at) {
        return new OutboxEvent(id, aggregateType, aggregateId, eventType,
                payload, headers, createdAt, at);
    }

    public boolean isPublished() {
        return publishedAt != null;
    }
}
