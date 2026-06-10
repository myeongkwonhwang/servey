package io.github.orange2652.partner.channel.core.saga.confirmedOrder;

import io.github.orange2652.partner.channel.gateway.logistics.LogisticsGateway;
import io.github.orange2652.partner.channel.gateway.logistics.ShipmentRequest;
import io.github.orange2652.partner.channel.gateway.logistics.ShipmentResponse;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 학습 단계 Mock — 항상 OK 응답, shipmentId 는 UUID 로 새로 생성.
 *
 * <p>실제 자사 물류 시스템 연동은 Phase 1+ 라운드에서 결정 (DB connection / interface table polling /
 * Kafka 발행 중 선택).</p>
 */
@Slf4j
@Component
class MockLogisticsGateway implements LogisticsGateway {

    @Override
    public ShipmentResponse request(ShipmentRequest request) {
        String shipmentId = "MOCK-" + UUID.randomUUID();
        log.info("mock logistics request channel={} externalOrderProductId={} → shipmentId={}",
                request.channel(), request.externalOrderProductId(), shipmentId);
        return new ShipmentResponse(shipmentId);
    }
}
