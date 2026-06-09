package io.github.orange2652.partner.channel.batch.external.toss;

import io.github.orange2652.partner.channel.client.toss.TossOrderClient;
import io.github.orange2652.partner.channel.client.toss.TossOrderItem;
import io.github.orange2652.partner.channel.client.toss.TossOrderPage;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 학습 단계 Mock — 매 라운드 dummy 1건 반환 (orderProductId 단조 증가).
 * Outbox 적재 / Publisher 발행 흐름 검증용.
 * 실제 Feign 구현 라운드에서 @Primary 또는 Profile 분리로 교체.
 */
@Slf4j
@Component
class MockTossOrderClient implements TossOrderClient {

    private final AtomicLong counter = new AtomicLong(1L);

    @Override
    public TossOrderPage fetch(Instant windowStart, Instant windowEnd, String nextCursor, int limit) {
        long opid = counter.getAndIncrement();
        long productId = 100L + opid;
        Instant ordered = Instant.now();
        String raw = """
                {"orderId":%d,"orderProductId":%d,"productId":%d,"orderProductStatus":"PAID","orderedAt":"%s"}\
                """.formatted(opid, opid, productId, ordered);

        TossOrderItem item = new TossOrderItem(opid, opid, productId, "PAID", ordered, raw);

        log.info("mock toss fetch window=({}~{}) nextCursor={} → 1 dummy (orderProductId={})",
                windowStart, windowEnd, nextCursor, opid);
        return new TossOrderPage(List.of(item), null);
    }
}
