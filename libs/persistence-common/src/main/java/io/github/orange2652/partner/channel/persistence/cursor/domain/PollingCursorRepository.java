package io.github.orange2652.partner.channel.persistence.cursor.domain;

import java.util.Optional;

public interface PollingCursorRepository {

    Optional<PollingCursor> find(String channel, String resource);

    PollingCursor save(PollingCursor cursor);
}
