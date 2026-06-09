package io.github.orange2652.partner.channel.persistence.idempotency.infra;

import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEvent;
import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class ProcessedEventRepositoryAdapter implements ProcessedEventRepository {

    private final ProcessedEventJpaRepository jpaRepository;

    @Override
    public boolean exists(String consumerName, String eventId) {
        return jpaRepository.existsById(new ProcessedEventId(consumerName, eventId));
    }

    @Override
    public ProcessedEvent save(ProcessedEvent event) {
        return jpaRepository.save(ProcessedEventJpaEntity.from(event)).toDomain();
    }
}
