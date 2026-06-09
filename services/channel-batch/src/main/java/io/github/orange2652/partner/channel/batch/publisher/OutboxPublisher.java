package io.github.orange2652.partner.channel.batch.publisher;

import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxEvent;
import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxRepository;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Outbox → Kafka 발행 (Polling Publisher = Message Relay).
 *
 * 한 라운드:
 *   1. outbox.findUnpublished(BATCH_SIZE)
 *   2. for each: KafkaTemplate.send(topic, key=aggregateId, value=payload)
 *      - send().get() 로 ack 대기 (blocking) → 확정 후 markPublished
 *   3. 실패 시 break — 다음 라운드 재시도 (published_at 이 null 이라 자동 retry)
 *
 * 미적용 (다음 라운드):
 *   - 다중 인스턴스 동시성 제어 (ShedLock / SELECT ... FOR UPDATE SKIP LOCKED)
 *   - 토픽 라우팅 by aggregate_type / event_type (현재는 단일 토픽)
 *   - dead letter (publish 영구 실패 시)
 */
@Slf4j
@Component
@RequiredArgsConstructor
class OutboxPublisher {

    static final int BATCH_SIZE = 50;
    static final long SEND_TIMEOUT_SECONDS = 5L;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxTopicRouter topicRouter;

    @Scheduled(fixedDelay = 5_000L)
    void publish() {
        List<OutboxEvent> events = outboxRepository.findUnpublished(BATCH_SIZE);
        if (events.isEmpty()) {
            return;
        }
        int published = 0;
        for (OutboxEvent event : events) {
            if (!sendOne(event)) {
                break;
            }
            outboxRepository.markPublished(event.id(), Instant.now());
            published++;
        }
        log.info("outbox publish round picked={} published={}", events.size(), published);
    }

    private boolean sendOne(OutboxEvent event) {
        String topic = topicRouter.resolve(event.aggregateType(), event.eventType());
        try {
            kafkaTemplate.send(topic, event.aggregateId(), event.payload())
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("outbox publish interrupted id={}", event.id());
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.info("outbox publish failed id={} topic={} eventType={} reason={}",
                    event.id(), topic, event.eventType(), e.getMessage());
            return false;
        }
    }
}
