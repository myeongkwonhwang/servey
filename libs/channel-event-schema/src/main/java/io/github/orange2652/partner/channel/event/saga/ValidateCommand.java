package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

/**
 * A1 step 2 — validate command payload (판매가능 검증 요청).
 *
 * <p>saga-orchestrator 가 발행 → service-core 가 수신해 채널별 룰로 판매가능 여부 판정.</p>
 *
 * <p><b>책임 경계</b>: read-only step. service-core 는 DB 쓰지 않음. 외부 raw 의
 * 정규화 필드 (예: TOSS 의 {@code orderProductStatus}) 만 보고 룰 적용.</p>
 *
 * @param channel 채널 식별자 (예: {@code "TOSS"}) — 채널별 룰 분기
 * @param raw     채널 응답 원본 (JSON 문자열) — saga state.payload 그대로
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ValidateCommand(
        String channel,
        String raw
) {
    public ValidateCommand {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(raw, "raw");
    }
}
