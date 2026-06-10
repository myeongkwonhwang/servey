package io.github.orange2652.partner.channel.adapter.external.toss;

import io.github.orange2652.partner.channel.client.toss.CancelOrderRequest;
import io.github.orange2652.partner.channel.client.toss.CancelOrderResult;
import io.github.orange2652.partner.channel.client.toss.TossOrderCancelClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 학습 단계 Mock — 항상 OK 응답. 실제 Feign 구현은 Phase 1+.
 *
 * <p>step 2 FAIL 시 step 1 보상 흐름 검증용.</p>
 */
@Slf4j
@Component
class MockTossOrderCancelClient implements TossOrderCancelClient {

    @Override
    public CancelOrderResult cancel(long orderProductId, CancelOrderRequest request) {
        log.info("mock toss seller-cancel orderProductId={} penalty={} reason={} → OK",
                orderProductId, request.deliveryPenaltyCharger(), request.reason());
        return CancelOrderResult.ok();
    }
}
