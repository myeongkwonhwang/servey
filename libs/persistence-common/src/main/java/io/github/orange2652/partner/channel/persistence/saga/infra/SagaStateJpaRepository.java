package io.github.orange2652.partner.channel.persistence.saga.infra;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository — package-private.
 */
interface SagaStateJpaRepository extends JpaRepository<SagaStateJpaEntity, UUID> {

    Optional<SagaStateJpaEntity> findBySagaTypeAndCorrelationKey(String sagaType, String correlationKey);
}
