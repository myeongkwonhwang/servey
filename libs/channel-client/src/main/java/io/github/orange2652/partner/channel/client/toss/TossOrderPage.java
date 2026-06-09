package io.github.orange2652.partner.channel.client.toss;

import java.util.List;
import java.util.Objects;

/**
 * 토스 주문 조회 cursor 페이지네이션 한 페이지.
 * nextCursor 가 null 이면 현재 윈도우의 마지막 페이지.
 */
public record TossOrderPage(
        List<TossOrderItem> results,
        String nextCursor
) {
    public TossOrderPage {
        Objects.requireNonNull(results, "results");
        results = List.copyOf(results);
    }

    public static TossOrderPage empty() {
        return new TossOrderPage(List.of(), null);
    }
}
