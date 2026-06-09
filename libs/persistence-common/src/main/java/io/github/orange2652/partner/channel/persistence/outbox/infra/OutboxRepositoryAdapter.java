package io.github.orange2652.partner.channel.persistence.outbox.infra;

import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxEvent;
import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxRepository;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
class OutboxRepositoryAdapter implements OutboxRepository {

    private final OutboxJpaRepository jpaRepository;

    @Override
    public OutboxEvent save(OutboxEvent event) {
        return jpaRepository.save(OutboxJpaEntity.from(event)).toDomain();
    }

    @Override
    public List<OutboxEvent> findUnpublished(int limit) {
        return jpaRepository.findByPublishedAtIsNullOrderByIdAsc(PageRequest.of(0, limit))
                .stream()
                .map(OutboxJpaEntity::toDomain)
                .toList();
    }

    @Override
    @Transactional
    public void markPublished(Long id, Instant publishedAt) {
        jpaRepository.updatePublishedAt(id, publishedAt);
    }
}
