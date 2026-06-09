package io.github.orange2652.partner.channel.persistence.outbox.infra;

import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface OutboxJpaRepository extends JpaRepository<OutboxJpaEntity, Long> {

    List<OutboxJpaEntity> findByPublishedAtIsNullOrderByIdAsc(Pageable pageable);

    @Modifying
    @Query("UPDATE OutboxJpaEntity o SET o.publishedAt = :publishedAt WHERE o.id = :id")
    int updatePublishedAt(@Param("id") Long id, @Param("publishedAt") Instant publishedAt);
}
