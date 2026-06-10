package io.github.orange2652.partner.channel.persistence.idempotency.domain;

/**
 * Consumer 멱등성 Port — 동일 {@code (consumerName, eventId)} 두 번 처리 방지.
 *
 * <p>at-least-once delivery 환경 (Kafka) 에서 동일 메시지가 두 번 도달하더라도 도메인 변경은
 * 한 번만 일어나도록, 메시지 처리와 같은 Tx 안에서 {@link #save} 한다 (Microservices Patterns Ch.3.3.2).</p>
 */
public interface ProcessedEventRepository {

    boolean exists(String consumerName, String eventId);

    /**
     * 처리 완료 마킹. {@code (consumer_name, event_id)} PK 라 중복 INSERT 는 DB 가 거부.
     */
    ProcessedEvent save(ProcessedEvent event);
}
