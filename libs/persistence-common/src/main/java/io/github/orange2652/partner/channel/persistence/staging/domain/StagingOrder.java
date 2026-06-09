package io.github.orange2652.partner.channel.persistence.staging.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * A1 saga step 1 의 결과 — 외부 raw 주문을 비정규 schema 에 적재한 row.
 * raw 는 외부 응답 그대로 JSON 보존, typed 필드는 자주 조회되는 식별자/상태/시간만.
 */
public record StagingOrder(
        Long id,
        String channel,
        String externalOrderId,
        String externalOrderProductId,
        String externalStatus,
        Instant orderedAt,
        String raw,
        Instant receivedAt
) {
    public StagingOrder {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(externalOrderId, "externalOrderId");
        Objects.requireNonNull(externalOrderProductId, "externalOrderProductId");
        Objects.requireNonNull(externalStatus, "externalStatus");
        Objects.requireNonNull(orderedAt, "orderedAt");
        Objects.requireNonNull(raw, "raw");
    }

    public static StagingOrder newRecord(String channel,
                                         String externalOrderId,
                                         String externalOrderProductId,
                                         String externalStatus,
                                         Instant orderedAt,
                                         String raw) {
        return new StagingOrder(null, channel, externalOrderId, externalOrderProductId,
                externalStatus, orderedAt, raw, Instant.now());
    }
}
