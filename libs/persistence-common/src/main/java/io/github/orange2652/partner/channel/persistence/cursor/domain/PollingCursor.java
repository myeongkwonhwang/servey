package io.github.orange2652.partner.channel.persistence.cursor.domain;

import java.time.LocalDateTime;
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
        LocalDateTime windowStart,
        LocalDateTime windowEnd,
        String nextCursor,
        LocalDateTime updatedAt
) {
    public PollingCursor {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(windowStart, "windowStart");
        Objects.requireNonNull(windowEnd, "windowEnd");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    /**
     * 최초 폴링 — 윈도우만 설정, {@code nextCursor=null}.
     */
    public static PollingCursor initial(String channel, String resource,
                                        LocalDateTime windowStart, LocalDateTime windowEnd) {
        return new PollingCursor(channel, resource, windowStart, windowEnd, null, LocalDateTime.now());
    }

    /**
     * 같은 윈도우 안 다음 페이지로 진행 — {@code nextCursor} 갱신.
     */
    public PollingCursor advance(String nextCursor) {
        return new PollingCursor(channel, resource, windowStart, windowEnd, nextCursor, LocalDateTime.now());
    }

    /**
     * 다음 윈도우로 전환 — 윈도우 갱신 + {@code nextCursor=null} 리셋.
     * 호출자 책임: 윈도우를 갭 없이 이어붙임 (이전 {@code windowEnd} 를 다음 {@code windowStart} 로).
     */
    public PollingCursor rollWindow(LocalDateTime newWindowStart, LocalDateTime newWindowEnd) {
        return new PollingCursor(channel, resource, newWindowStart, newWindowEnd, null, LocalDateTime.now());
    }

    /**
     * 현재 윈도우의 모든 페이지를 다 가져왔는지 — {@code nextCursor IS NULL}.
     */
    public boolean windowCompleted() {
        return nextCursor == null;
    }
}
