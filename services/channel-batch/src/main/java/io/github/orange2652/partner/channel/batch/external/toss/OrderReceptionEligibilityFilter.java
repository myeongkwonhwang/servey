package io.github.orange2652.partner.channel.batch.external.toss;

import io.github.orange2652.partner.channel.client.toss.TossOrderItem;
import java.time.Duration;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;

/**
 * A1 saga 시작 자격 필터 — orderedAt + {@link #RECEPTION_DELAY} 가 경과한 주문만 통과.
 *
 * <p><b>이유</b>: 결제 직후 30분 동안은 구매자 변심 / 빠른 취소 가능성 ↑.
 * 그 기간이 지난 후 saga 를 시작하여 외부 PREPARING_PRODUCT 전이 (= 셀러 페널티 영역 진입) 를 늦춤.</p>
 *
 * <p><b>cursor 정합성</b>: {@link TossOrderPollingService} 에서 {@code windowEnd = now - RECEPTION_DELAY}
 * 로 좁히는 정책과 함께 동작. 본 필터는 외부 응답 안 30분 미경과 항목에 대한 안전망.</p>
 *
 * <p>학습 단계 상수. 추후 application.yml configurable 로 확장 가능.</p>
 */
@Component
public class OrderReceptionEligibilityFilter {

    static final Duration RECEPTION_DELAY = Duration.ofMinutes(30);

    public boolean isEligible(TossOrderItem item) {
        return isEligible(item.orderedAt(), LocalDateTime.now());
    }

    /**
     * @param orderedAt 주문 시각 (외부 응답의 orderedAt)
     * @param now       현재 시각 — 테스트 가능성 위해 인자로 받음
     */
    boolean isEligible(LocalDateTime orderedAt, LocalDateTime now) {
        return !orderedAt.isAfter(now.minus(RECEPTION_DELAY));
    }
}
