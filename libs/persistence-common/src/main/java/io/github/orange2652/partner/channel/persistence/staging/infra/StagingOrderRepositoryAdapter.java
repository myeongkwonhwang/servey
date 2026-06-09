package io.github.orange2652.partner.channel.persistence.staging.infra;

import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrder;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class StagingOrderRepositoryAdapter implements StagingOrderRepository {

    private final StagingOrderJpaRepository jpaRepository;

    @Override
    public StagingOrder save(StagingOrder order) {
        return jpaRepository.save(StagingOrderJpaEntity.from(order)).toDomain();
    }
}
