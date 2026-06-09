package io.github.orange2652.partner.channel.persistence.idempotency.infra;

import org.springframework.data.jpa.repository.JpaRepository;

interface ProcessedEventJpaRepository extends JpaRepository<ProcessedEventJpaEntity, ProcessedEventId> {
}
