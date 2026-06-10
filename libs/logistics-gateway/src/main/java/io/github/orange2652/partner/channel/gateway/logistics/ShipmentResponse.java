package io.github.orange2652.partner.channel.gateway.logistics;

import java.util.Objects;

/**
 * 자사 물류 시스템 송장/배송 의뢰 응답.
 *
 * @param shipmentId 자사 물류 시스템이 부여한 의뢰 식별자
 */
public record ShipmentResponse(
        String shipmentId
) {
    public ShipmentResponse {
        Objects.requireNonNull(shipmentId, "shipmentId");
    }
}
