package io.github.orange2652.partner.channel.persistence.idempotency.infra;

import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "processed_event")   // schema 명시 X — 각 서비스의 hibernate.default_schema 사용 (channel_schema / core_schema)
@IdClass(ProcessedEventId.class)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
class ProcessedEventJpaEntity {

    @Id
    @Column(name = "consumer_name", nullable = false, length = 64)
    private String consumerName;

    @Id
    @Column(name = "event_id", nullable = false, length = 64)
    private String eventId;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    static ProcessedEventJpaEntity from(ProcessedEvent event) {
        return ProcessedEventJpaEntity.builder()
                .consumerName(event.consumerName())
                .eventId(event.eventId())
                .processedAt(event.processedAt())
                .build();
    }

    ProcessedEvent toDomain() {
        return new ProcessedEvent(consumerName, eventId, processedAt);
    }
}
