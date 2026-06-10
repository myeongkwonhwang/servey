package io.github.orange2652.partner.channel.client.toss;

import java.util.Objects;

/**
 * 토스 판매자 취소 요청.
 *
 * @param deliveryPenaltyCharger 배송비 부과 책임자 — {@code "USER"} / {@code "MERCHANT"}
 * @param reason                 취소 사유 (100자 이내)
 * @param detailReason           상세 사유 (250자 이내, optional)
 */
public record CancelOrderRequest(
        String deliveryPenaltyCharger,
        String reason,
        String detailReason
) {
    public CancelOrderRequest {
        Objects.requireNonNull(deliveryPenaltyCharger, "deliveryPenaltyCharger");
        Objects.requireNonNull(reason, "reason");
    }

    public static CancelOrderRequest sagaCompensation(String reason) {
        return new CancelOrderRequest("MERCHANT", reason, "saga step 2 validate FAIL");
    }
}
