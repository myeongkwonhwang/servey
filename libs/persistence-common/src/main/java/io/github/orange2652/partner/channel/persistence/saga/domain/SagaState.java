package io.github.orange2652.partner.channel.persistence.saga.domain;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * Saga 인스턴스의 영속 상태 — saga log entry.
 *
 * <p>{@code saga_schema.saga_state} 와 1:1 매핑되는 순수 도메인 record. JPA / Spring 의존 없음.</p>
 *
 * <p><b>중복 시작 방지</b>: {@code (sagaType, correlationKey)} UNIQUE 제약 — 같은 외부 주문에
 * 대해 동일 saga 가 두 번 INSERT 시도되면 DB 가 거부 ({@link org.springframework.dao.DataIntegrityViolationException}).</p>
 *
 * <p><b>상태 전이</b>는 {@link #advance(String)} / {@link #compensate()} / {@link #complete()} /
 * {@link #abort()} 가 새 인스턴스를 반환 (불변).</p>
 *
 * @param sagaId         saga 인스턴스 식별자 (UUID, PK)
 * @param sagaType       saga 종류 식별자 (예: {@code "A1_ORDER_RECEPTION"})
 * @param correlationKey 외부 식별자 — 채널 응답의 자연키 (예: orderProductId)
 * @param currentStep    현재 진행 중인 step 이름 (예: {@code "STARTED"}, {@code "STAGING_INSERTED"})
 * @param status         {@link SagaStatus} — lifecycle 상태
 * @param payload        saga context (JSON 문자열) — 원본 외부 응답 등
 * @param startedAt      saga 시작 시각
 * @param updatedAt      마지막 상태 전이 시각
 */
public record SagaState(
        UUID sagaId,
        String sagaType,
        String correlationKey,
        String currentStep,
        SagaStatus status,
        String payload,
        LocalDateTime startedAt,
        LocalDateTime updatedAt
) {
    public SagaState {
        Objects.requireNonNull(sagaId, "sagaId");
        Objects.requireNonNull(sagaType, "sagaType");
        Objects.requireNonNull(correlationKey, "correlationKey");
        Objects.requireNonNull(currentStep, "currentStep");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    /**
     * 새 saga 인스턴스 — {@code status=RUNNING}, {@code sagaId} 자동 생성, 시작/갱신 시각은 {@link LocalDateTime#now()}.
     *
     * @param sagaType       saga 종류 식별자
     * @param correlationKey 외부 식별자
     * @param initialStep    초기 step 이름 (예: {@code "STARTED"})
     * @param payload        saga context (JSON 문자열)
     * @return 새 {@link SagaState} 인스턴스
     */
    public static SagaState start(String sagaType,
                                  String correlationKey,
                                  String initialStep,
                                  String payload) {
        LocalDateTime now = LocalDateTime.now();
        return new SagaState(UUID.randomUUID(), sagaType, correlationKey,
                initialStep, SagaStatus.RUNNING, payload, now, now);
    }

    /**
     * 다음 step 으로 전이 — {@code status=RUNNING} 유지, {@code currentStep} 갱신.
     */
    public SagaState advance(String nextStep) {
        return new SagaState(sagaId, sagaType, correlationKey, nextStep,
                SagaStatus.RUNNING, payload, startedAt, LocalDateTime.now());
    }

    /**
     * 보상 시작 — {@code status=COMPENSATING}.
     */
    public SagaState compensate() {
        return new SagaState(sagaId, sagaType, correlationKey, currentStep,
                SagaStatus.COMPENSATING, payload, startedAt, LocalDateTime.now());
    }

    /**
     * 정상 종료 — {@code status=COMPLETED}.
     */
    public SagaState complete() {
        return new SagaState(sagaId, sagaType, correlationKey, currentStep,
                SagaStatus.COMPLETED, payload, startedAt, LocalDateTime.now());
    }

    /**
     * 비정상 종료 — {@code status=ABORTED}.
     */
    public SagaState abort() {
        return new SagaState(sagaId, sagaType, correlationKey, currentStep,
                SagaStatus.ABORTED, payload, startedAt, LocalDateTime.now());
    }
}
