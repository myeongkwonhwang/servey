package io.github.orange2652.partner.channel.common;

import java.util.Optional;

/**
 * 외부 채널 식별자. 본 프로젝트가 지원하는 멀티채널 enum.
 *
 * <p>현재 1 채널 (TOSS) 만 정의. 다채널 라운드에서 NAVER / YOUTUBE_SHOPPING / TOSS_SHOPPING 등 추가
 * 예정.</p>
 *
 * <p><b>직렬화 정책</b>: 외부와의 통신 (Kafka payload, DB 컬럼) 은 enum 이름 ({@code name()}) 을
 * code 로 사용. record 필드는 점진 전환을 위해 당분간 {@code String channel} 유지 — 비교/검증
 * 시에만 본 enum 으로 변환한다 ({@link #fromCode}).</p>
 */
public enum Channel {

    TOSS;

    /**
     * @return 일치하는 채널, 미지원 / null 이면 {@link Optional#empty()}.
     *         호출자가 도메인 예외 (예: {@code UnsupportedChannelException}) 로 wrap.
     */
    public static Optional<Channel> fromCode(String code) {
        if (code == null) {
            return Optional.empty();
        }
        for (Channel c : values()) {
            if (c.name().equals(code)) {
                return Optional.of(c);
            }
        }
        return Optional.empty();
    }

    public String code() {
        return name();
    }
}
