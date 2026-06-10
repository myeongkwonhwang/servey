package io.github.orange2652.partner.channel.adapter.external.toss;

import io.github.orange2652.partner.channel.client.toss.OrderProductStatusChangeResult;
import io.github.orange2652.partner.channel.client.toss.TossOrderStatusClient;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 학습 단계 Mock — 항상 성공 응답. 실제 Feign 구현은 Phase 1+.
 *
 * <p>A1 step 1 의 PAID → PREPARING_PRODUCT 전이를 학습 흐름에서 검증하기 위한 stub.</p>
 */
@Slf4j
@Component
class MockTossOrderStatusClient implements TossOrderStatusClient {

    @Override
    public OrderProductStatusChangeResult changeStatus(List<Long> orderProductIds, String status) {
        log.info("mock toss status change orderProductIds={} status={} → all success",
                orderProductIds, status);
        return OrderProductStatusChangeResult.success(orderProductIds.size());
    }
}
