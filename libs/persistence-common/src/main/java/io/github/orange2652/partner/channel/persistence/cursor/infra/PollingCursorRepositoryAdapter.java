package io.github.orange2652.partner.channel.persistence.cursor.infra;

import io.github.orange2652.partner.channel.persistence.cursor.domain.PollingCursor;
import io.github.orange2652.partner.channel.persistence.cursor.domain.PollingCursorRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class PollingCursorRepositoryAdapter implements PollingCursorRepository {

    private final PollingCursorJpaRepository jpaRepository;

    @Override
    public Optional<PollingCursor> find(String channel, String resource) {
        return jpaRepository.findById(new PollingCursorId(channel, resource))
                .map(PollingCursorJpaEntity::toDomain);
    }

    @Override
    public PollingCursor save(PollingCursor cursor) {
        return jpaRepository.save(PollingCursorJpaEntity.from(cursor)).toDomain();
    }
}
