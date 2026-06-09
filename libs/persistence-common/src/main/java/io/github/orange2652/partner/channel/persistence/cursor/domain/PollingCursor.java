package io.github.orange2652.partner.channel.persistence.cursor.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * 외부 채널 폴링 진행 위치.
 * 토스 주문 조회 v2 와 같은 (startDate, endDate) + cursor 페이지네이션 호환.
 *
 * @param channel    채널 식별자 (예: "TOSS")
 * @param resource   리소스 종류 (예: "ORDER")
 * @param windowStart 현재 폴링 윈도우 시작 시각
 * @param windowEnd  현재 폴링 윈도우 종료 시각
 * @param nextCursor 페이지네이션 진행 중 cursor (null = 윈도우 완료, 다음 라운드로)
 * @param updatedAt  마지막 갱신 시각
 */
public record PollingCursor(
        String channel,
        String resource,
        Instant windowStart,
        Instant windowEnd,
        String nextCursor,
        Instant updatedAt
) {
    public PollingCursor {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(windowStart, "windowStart");
        Objects.requireNonNull(windowEnd, "windowEnd");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    public static PollingCursor initial(String channel, String resource,
                                        Instant windowStart, Instant windowEnd) {
        return new PollingCursor(channel, resource, windowStart, windowEnd, null, Instant.now());
    }

    public PollingCursor advance(String nextCursor) {
        return new PollingCursor(channel, resource, windowStart, windowEnd, nextCursor, Instant.now());
    }

    public PollingCursor rollWindow(Instant newWindowStart, Instant newWindowEnd) {
        return new PollingCursor(channel, resource, newWindowStart, newWindowEnd, null, Instant.now());
    }

    public boolean windowCompleted() {
        return nextCursor == null;
    }
}
