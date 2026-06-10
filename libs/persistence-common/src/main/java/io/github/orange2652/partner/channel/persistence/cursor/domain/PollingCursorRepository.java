package io.github.orange2652.partner.channel.persistence.cursor.domain;

import java.util.Optional;

/**
 * 외부 채널 폴링 진행 위치 Port.
 *
 * <p>{@code (channel, resource)} 가 자연 키 — 채널/리소스 당 1 row.</p>
 */
public interface PollingCursorRepository {

    /**
     * 진행 위치 조회. 최초 폴링이면 {@link Optional#empty()} — 호출자가
     * {@link PollingCursor#initial} 로 초기 윈도우를 생성한다.
     */
    Optional<PollingCursor> find(String channel, String resource);

    /**
     * 진행 위치 INSERT 또는 UPDATE — 같은 자연 키 row 가 이미 있으면 UPDATE.
     * 외부 응답 처리와 같은 Tx 안에서 호출해야 페이지 누락/중복이 사라진다.
     */
    PollingCursor save(PollingCursor cursor);
}
