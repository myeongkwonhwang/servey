package io.github.orange2652.partner.channel.persistence.saga.infra;

import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * JPA Entity — {@code saga_schema.saga_state} 매핑.
 *
 * <p>package-private — 외부에서는 도메인 record ({@link SagaState}) + Port 로만 접근.</p>
 */
@Entity
@Table(name = "saga_state", schema = "saga_schema")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
class SagaStateJpaEntity {

    @Id
    @Column(name = "saga_id", nullable = false)
    private UUID sagaId;

    @Column(name = "saga_type", nullable = false, length = 64)
    private String sagaType;

    @Column(name = "correlation_key", nullable = false, length = 128)
    private String correlationKey;

    @Column(name = "current_step", nullable = false, length = 64)
    private String currentStep;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private SagaStatus status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "started_at", nullable = false)
    private LocalDateTime startedAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    static SagaStateJpaEntity from(SagaState state) {
        return SagaStateJpaEntity.builder()
                .sagaId(state.sagaId())
                .sagaType(state.sagaType())
                .correlationKey(state.correlationKey())
                .currentStep(state.currentStep())
                .status(state.status())
                .payload(state.payload())
                .startedAt(state.startedAt())
                .updatedAt(state.updatedAt())
                .build();
    }

    SagaState toDomain() {
        return new SagaState(sagaId, sagaType, correlationKey, currentStep,
                status, payload, startedAt, updatedAt);
    }
}
