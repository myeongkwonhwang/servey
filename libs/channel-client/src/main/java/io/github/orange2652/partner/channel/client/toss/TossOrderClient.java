package io.github.orange2652.partner.channel.client.toss;

import java.time.Instant;

/**
 * 토스 주문 조회 Port.
 * 실제 Feign 구현은 별도 라운드, 현재는 학습용 Mock 구현 (channel-batch 안).
 *
 * 외부 API: GET /api/v3/shopping-fep/orders/v2
 * - 페이지네이션: cursor 기반 (nextCursor)
 * - 윈도우: (startDate, endDate) 필수, 최대 31일
 * - limit: 1~50
 */
public interface TossOrderClient {

    TossOrderPage fetch(Instant windowStart, Instant windowEnd, String nextCursor, int limit);
}
