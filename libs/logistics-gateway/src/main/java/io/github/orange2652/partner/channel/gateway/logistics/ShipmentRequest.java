package io.github.orange2652.partner.channel.gateway.logistics;

import java.util.Objects;

/**
 * 자사 물류 시스템 송장/배송 의뢰 요청.
 *
 * @param channel                채널 식별자 (예: {@code "TOSS"})
 * @param externalOrderProductId 외부 주문 상품 식별자 — 외부 시스템에서 멱등성 키로 사용
 */
public record ShipmentRequest(
        String channel,
        String externalOrderProductId
) {
    public ShipmentRequest {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(externalOrderProductId, "externalOrderProductId");
    }
}
