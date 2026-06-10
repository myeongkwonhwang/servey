package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A1 step 3 — confirmedOrder reply payload.
 *
 * <p>service-core 가 발행 → saga-orchestrator 가 수신해 saga state advance.</p>
 *
 * @param result       OK / FAILED
 * @param orderId      OK 시 내부 주문 PK
 * @param shipmentId   OK 시 외부 물류 식별자
 * @param errorCode    FAILED 시 식별 코드
 * @param errorMessage FAILED 시 상세
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConfirmedOrderReply(
        Result result,
        Long orderId,
        String shipmentId,
        String errorCode,
        String errorMessage
) {
    public enum Result {
        OK, FAILED
    }

    public static ConfirmedOrderReply ok(Long orderId, String shipmentId) {
        return new ConfirmedOrderReply(Result.OK, orderId, shipmentId, null, null);
    }

    public static ConfirmedOrderReply failed(String errorCode, String errorMessage) {
        return new ConfirmedOrderReply(Result.FAILED, null, null, errorCode, errorMessage);
    }
}
