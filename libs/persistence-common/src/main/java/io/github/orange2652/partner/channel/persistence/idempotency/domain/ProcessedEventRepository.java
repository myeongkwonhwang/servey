package io.github.orange2652.partner.channel.persistence.idempotency.domain;

public interface ProcessedEventRepository {

    boolean exists(String consumerName, String eventId);

    ProcessedEvent save(ProcessedEvent event);
}
