package io.github.orange2652.partner.channel.adapter.external.toss;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.LocalDateTime;

/**
 * 토스 주문 조회 v2 응답의 한 건 (Kafka payload).
 * 외부에 더 많은 필드가 있으나 staging_order 의 typed 컬럼에 필요한 것만 매핑.
 * 원본 JSON 전체는 staging_order.raw 에 그대로 보존.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
record TossOrderPayload(
        Long orderId,
        Long orderProductId,
        Long productId,
        String orderProductStatus,
        LocalDateTime orderedAt
) {
}
