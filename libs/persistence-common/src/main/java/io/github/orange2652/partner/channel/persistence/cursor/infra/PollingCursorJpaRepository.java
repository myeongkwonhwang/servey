package io.github.orange2652.partner.channel.persistence.cursor.infra;

import org.springframework.data.jpa.repository.JpaRepository;

interface PollingCursorJpaRepository extends JpaRepository<PollingCursorJpaEntity, PollingCursorId> {
}
