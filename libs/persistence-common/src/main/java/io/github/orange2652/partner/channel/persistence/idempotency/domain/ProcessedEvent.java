package io.github.orange2652.partner.channel.persistence.idempotency.domain;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Consumer 멱등성 — 동일 {@code (consumer_name, event_id)} 두 번 처리 방지.
 *
 * <p>{@code (consumer_name, event_id)} 가 PK. 도메인 변경과 같은 Tx 안에서 INSERT 하면
 * at-least-once 환경에서도 도메인 변경은 한 번만 적용됨 (Microservices Patterns Ch.3.3.2).</p>
 *
 * @param consumerName 처리 주체 식별자 (예: {@code "channel-adapter"})
 * @param eventId      메시지 식별자 — 본 프로젝트는 {@code "{partition}:{offset}"} 사용
 * @param processedAt  처리 완료 시각
 */
public record ProcessedEvent(
        String consumerName,
        String eventId,
        LocalDateTime processedAt
) {
    public ProcessedEvent {
        Objects.requireNonNull(consumerName, "consumerName");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(processedAt, "processedAt");
    }

    /**
     * 처리 완료 row — {@code processedAt=now}.
     */
    public static ProcessedEvent newRecord(String consumerName, String eventId) {
        return new ProcessedEvent(consumerName, eventId, LocalDateTime.now());
    }
}
