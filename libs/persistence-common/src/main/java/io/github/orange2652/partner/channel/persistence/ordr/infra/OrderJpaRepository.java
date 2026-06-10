package io.github.orange2652.partner.channel.persistence.ordr.infra;

import org.springframework.data.jpa.repository.JpaRepository;

interface OrderJpaRepository extends JpaRepository<OrderJpaEntity, Long> {
}
