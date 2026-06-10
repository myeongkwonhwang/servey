package io.github.orange2652.partner.channel.client.toss;

import java.util.List;
import java.util.Objects;

/**
 * 토스 주문 상품 상태 변경 API 응답.
 *
 * @param totalCount    전체 건수
 * @param failedCount   실패 건수 (0 이면 모두 성공)
 * @param failedReasons 실패 사유 목록
 */
public record OrderProductStatusChangeResult(
        int totalCount,
        int failedCount,
        List<String> failedReasons
) {
    public OrderProductStatusChangeResult {
        Objects.requireNonNull(failedReasons, "failedReasons");
        failedReasons = List.copyOf(failedReasons);
    }

    public boolean allSucceeded() {
        return failedCount == 0;
    }

    public static OrderProductStatusChangeResult success(int totalCount) {
        return new OrderProductStatusChangeResult(totalCount, 0, List.of());
    }
}
