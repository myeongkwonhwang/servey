package io.github.orange2652.partner.channel.persistence.saga.domain;

/**
 * Saga 인스턴스의 lifecycle 상태.
 *
 * <ul>
 *   <li>{@link #RUNNING} — 진행 중. step 실행 중이거나 reply 대기 중.</li>
 *   <li>{@link #COMPENSATING} — 어떤 step 실패로 인해 보상 진행 중.</li>
 *   <li>{@link #COMPLETED} — 모든 step 정상 종료.</li>
 *   <li>{@link #ABORTED} — 보상 완료 또는 비복구 실패로 종료.</li>
 * </ul>
 */
public enum SagaStatus {
    RUNNING,
    COMPENSATING,
    COMPLETED,
    ABORTED
}
