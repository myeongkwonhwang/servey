package io.github.orange2652.partner.channel.persistence.ordr.infra;

import io.github.orange2652.partner.channel.persistence.ordr.domain.Order;
import io.github.orange2652.partner.channel.persistence.ordr.domain.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class OrderRepositoryAdapter implements OrderRepository {

    private final OrderJpaRepository jpaRepository;

    @Override
    public Order save(Order order) {
        return jpaRepository.save(OrderJpaEntity.from(order)).toDomain();
    }
}
