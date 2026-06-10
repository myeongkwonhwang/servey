package io.github.orange2652.partner.channel.client.toss;

/**
 * 토스 판매자 직접 취소 Port.
 *
 * <p>외부 API: {@code POST /api/v3/shopping-fep/order-products/{orderProductId}/seller-cancel}</p>
 *
 * <p><b>특징 (공식 문서)</b></p>
 * <ul>
 *   <li>거의 모든 상태에서 즉시 {@code CANCELED_PAYMENT} 강제 전이 가능
 *       (BEFORE_PAYMENT / COMPLETED_RETURN 제외)</li>
 *   <li><b>상품준비중 (PREPARING_PRODUCT) 상태에서도 취소 가능 ★</b> — 본 saga step 2 실패 보상의 핵심</li>
 *   <li>셀러 페널티 4점 (운영 시 주의 — 학습 단계 무관)</li>
 *   <li>응답: resultType=SUCCESS/FAIL — success 시 본문 없음 ({})</li>
 * </ul>
 *
 * <p>본 프로젝트의 실제 호출은 Phase 1+ 라운드에서 Feign 으로 교체.</p>
 */
public interface TossOrderCancelClient {

    /**
     * 단건 취소 호출.
     *
     * @param orderProductId 외부 주문 상품 ID
     * @param request        취소 요청 (deliveryPenaltyCharger, reason, detailReason)
     * @return 취소 결과 (성공 / 실패 + 사유)
     */
    CancelOrderResult cancel(long orderProductId, CancelOrderRequest request);
}
