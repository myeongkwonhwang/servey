package io.github.orange2652.partner.channel.persistence.saga.infra;

import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStateRepository;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * {@link SagaStateRepository} 의 JPA 구현 — Domain ↔ JPA Entity 변환만 담당.
 */
@Component
@RequiredArgsConstructor
class SagaStateRepositoryAdapter implements SagaStateRepository {

    private final SagaStateJpaRepository jpaRepository;

    @Override
    public SagaState save(SagaState state) {
        return jpaRepository.save(SagaStateJpaEntity.from(state)).toDomain();
    }

    @Override
    public Optional<SagaState> findById(UUID sagaId) {
        return jpaRepository.findById(sagaId).map(SagaStateJpaEntity::toDomain);
    }

    @Override
    public Optional<SagaState> findByCorrelation(String sagaType, String correlationKey) {
        return jpaRepository.findBySagaTypeAndCorrelationKey(sagaType, correlationKey)
                .map(SagaStateJpaEntity::toDomain);
    }
}
