package io.github.orange2652.partner.channel.client.toss;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * 토스 주문 조회 v2 응답의 한 건.
 * raw 는 원본 JSON 그대로 보존 (외부 raw 보존 원칙 — Outbox payload / staging_order.raw 에 그대로 사용).
 * typed 필드는 자주 조회되는 식별자/상태/시간만.
 */
public record TossOrderItem(
        Long orderId,
        Long orderProductId,
        Long productId,
        String orderProductStatus,
        LocalDateTime orderedAt,
        String raw
) {
    public TossOrderItem {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(orderProductId, "orderProductId");
        Objects.requireNonNull(orderProductStatus, "orderProductStatus");
        Objects.requireNonNull(orderedAt, "orderedAt");
        Objects.requireNonNull(raw, "raw");
    }
}
