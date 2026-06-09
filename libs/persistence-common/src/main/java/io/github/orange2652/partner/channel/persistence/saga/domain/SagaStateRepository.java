package io.github.orange2652.partner.channel.persistence.saga.domain;

import java.util.Optional;
import java.util.UUID;

/**
 * Saga 영속 Port.
 *
 * <p>{@link #save(SagaState)} 는 신규 INSERT 또는 기존 UPDATE 둘 다 담당. INSERT 시 동일
 * {@code (sagaType, correlationKey)} 가 이미 있으면 DB UNIQUE 위반으로
 * {@link org.springframework.dao.DataIntegrityViolationException} 가 발생 — 호출자가 멱등성 처리.</p>
 */
public interface SagaStateRepository {

    SagaState save(SagaState state);

    Optional<SagaState> findById(UUID sagaId);

    Optional<SagaState> findByCorrelation(String sagaType, String correlationKey);
}
