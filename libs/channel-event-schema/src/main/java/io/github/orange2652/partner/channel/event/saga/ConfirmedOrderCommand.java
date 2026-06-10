package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

/**
 * A1 step 3 — confirmedOrder command payload (내부 주문 확정 + 외부 물류 전송 요청).
 *
 * <p>saga-orchestrator 가 발행 → service-core 가 수신해:</p>
 * <ol>
 *   <li>외부 물류 호출 (Tx 밖)</li>
 *   <li>내부 주문 INSERT (Tx 안) — Pivot 통과 시점</li>
 * </ol>
 *
 * @param channel 채널 식별자 (예: {@code "TOSS"})
 * @param raw     채널 응답 원본 (JSON 문자열) — saga state.payload 그대로
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConfirmedOrderCommand(
        String channel,
        String raw
) {
    public ConfirmedOrderCommand {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(raw, "raw");
    }
}
