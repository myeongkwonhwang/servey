package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

/**
 * step 2 (validate) 실패 시 step 1 (unconfirmedOrder) 보상 command payload.
 *
 * <p>saga-orchestrator 가 발행 → channel-adapter 가 수신해:</p>
 * <ol>
 *   <li>외부 cancel 호출 (Tx 밖) — PREPARING_PRODUCT → CANCELED_PAYMENT</li>
 *   <li>staging UPDATE (Tx) — status: ACTIVE → CANCELED</li>
 * </ol>
 *
 * @param channel                채널 식별자
 * @param externalOrderProductId 외부 주문 상품 ID (보상 대상 식별)
 * @param reason                 보상 사유 (외부 cancel API 로 전달)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record UnconfirmedOrderCompensateCommand(
        String channel,
        String externalOrderProductId,
        String reason
) {
    public UnconfirmedOrderCompensateCommand {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(externalOrderProductId, "externalOrderProductId");
        Objects.requireNonNull(reason, "reason");
    }
}
