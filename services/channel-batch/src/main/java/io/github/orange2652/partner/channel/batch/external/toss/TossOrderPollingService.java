package io.github.orange2652.partner.channel.batch.external.toss;

import io.github.orange2652.partner.channel.client.toss.TossOrderClient;
import io.github.orange2652.partner.channel.client.toss.TossOrderItem;
import io.github.orange2652.partner.channel.client.toss.TossOrderPage;
import io.github.orange2652.partner.channel.persistence.cursor.domain.PollingCursor;
import io.github.orange2652.partner.channel.persistence.cursor.domain.PollingCursorRepository;
import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxEvent;
import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * channel-batch 의 토스 주문 폴링 application service.
 *
 * 한 라운드:
 *   1. cursor 조회 (없으면 initial window = 직전 1h)
 *   2. 외부 API 호출 — Tx 밖
 *   3. WRITE Tx — outbox INSERT + cursor 갱신 (한 Tx atomic)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TossOrderPollingService {

    static final String CHANNEL = "TOSS";
    static final String RESOURCE = "ORDER";
    static final String AGGREGATE_TYPE = "ORDER";
    static final String EVENT_TYPE = "OrderReceived";
    static final int FETCH_LIMIT = 50;
    static final Duration INITIAL_WINDOW = Duration.ofHours(1);

    private final TossOrderClient tossOrderClient;
    private final OutboxRepository outboxRepository;
    private final PollingCursorRepository pollingCursorRepository;
    private final TransactionTemplate transactionTemplate;

    public void poll() {
        PollingCursor cursor = loadOrInitCursor();

        TossOrderPage page = tossOrderClient.fetch(
                cursor.windowStart(), cursor.windowEnd(), cursor.nextCursor(), FETCH_LIMIT);

        transactionTemplate.executeWithoutResult(status -> persistResults(cursor, page));

        log.info("toss order polled channel={} resource={} window=({}~{}) prevCursor={} fetched={} nextCursor={}",
                CHANNEL, RESOURCE, cursor.windowStart(), cursor.windowEnd(),
                cursor.nextCursor(), page.results().size(), page.nextCursor());
    }

    private PollingCursor loadOrInitCursor() {
        return pollingCursorRepository.find(CHANNEL, RESOURCE)
                .orElseGet(() -> {
                    Instant now = Instant.now();
                    return PollingCursor.initial(CHANNEL, RESOURCE, now.minus(INITIAL_WINDOW), now);
                });
    }

    private void persistResults(PollingCursor prev, TossOrderPage page) {
        for (TossOrderItem item : page.results()) {
            OutboxEvent event = OutboxEvent.newEvent(
                    AGGREGATE_TYPE,
                    item.orderProductId().toString(),
                    EVENT_TYPE,
                    item.raw(),
                    buildHeaders());
            outboxRepository.save(event);
        }
        pollingCursorRepository.save(nextCursor(prev, page));
    }

    private PollingCursor nextCursor(PollingCursor prev, TossOrderPage page) {
        if (page.nextCursor() != null) {
            return prev.advance(page.nextCursor());
        }
        // 윈도우 완료 → 다음 윈도우는 (prev.windowEnd, now). 갭 없이 이어붙임
        return prev.rollWindow(prev.windowEnd(), Instant.now());
    }

    private String buildHeaders() {
        return "{\"channel\":\"" + CHANNEL + "\",\"traceId\":\"" + UUID.randomUUID() + "\"}";
    }
}
