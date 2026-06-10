package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

/**
 * A1 step 1 — unconfirmedOrder command payload (외부 raw → 내부 미확정 적재 요청).
 *
 * <p>saga-orchestrator 가 발행 → channel-adapter 가 수신해 채널 raw 를 파싱·비정규 적재.</p>
 *
 * <p><b>책임 경계</b>: saga-orchestrator 는 채널 raw 의 형식을 알지 못한다 — channel 라벨과
 * raw JSON 만 전달. 정규화 (orderProductId 추출 등) 는 channel-adapter 의 채널별 파서가 담당.</p>
 *
 * @param channel 채널 식별자 (예: {@code "TOSS"})
 * @param raw     채널 응답 원본 (JSON 문자열) — staging.raw 에 그대로 저장
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record UnconfirmedOrderCommand(
        String channel,
        String raw
) {
    public UnconfirmedOrderCommand {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(raw, "raw");
    }
}
