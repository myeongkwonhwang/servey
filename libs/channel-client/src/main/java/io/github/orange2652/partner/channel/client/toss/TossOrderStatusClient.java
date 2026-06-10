package io.github.orange2652.partner.channel.client.toss;

import java.util.List;

/**
 * 토스 주문 상품 상태 변경 Port.
 *
 * <p>외부 API: {@code PUT /api/v3/shopping-fep/orders/products/status}</p>
 *
 * <p><b>허용 상태 전이 (공식 문서)</b></p>
 * <ul>
 *   <li>결제완료 (PAID) → 상품준비중 (PREPARING_PRODUCT)</li>
 *   <li>결제완료 → 발송지연 (DELAY_SHIPPING) / 결제취소 (CANCELED_PAYMENT)</li>
 *   <li>상품준비중 → 발송지연 / 결제취소</li>
 *   <li>발송지연 → 결제취소</li>
 *   <li>배송완료 → 결제취소</li>
 * </ul>
 *
 * <p>A1 step 1 의 외부 호출 — PAID → PREPARING_PRODUCT 전이가 유스케이스.</p>
 *
 * <p>본 프로젝트의 실제 호출은 Phase 1+ 라운드에서 Feign 으로 교체. 현재는 학습용 Mock.</p>
 *
 * <p>한 번에 최대 100개 일괄 변경 가능 (공식 문서). 본 프로젝트는 saga 단위 (1건) 호출로 시작.</p>
 */
public interface TossOrderStatusClient {

    /**
     * 주문 상품 상태 변경. 일부 실패하더라도 성공한 것은 변경됨.
     *
     * @param orderProductIds 주문 상품 ID 목록 (최대 100개)
     * @param status          {@code "PREPARING_PRODUCT"} / {@code "CANCELED_PAYMENT"} 등
     * @return 변경 결과 (totalCount / failedCount / failedReasons)
     */
    OrderProductStatusChangeResult changeStatus(List<Long> orderProductIds, String status);
}
