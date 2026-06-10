package io.github.orange2652.partner.channel.gateway.logistics;

/**
 * 자사 물류 시스템 송장/배송 의뢰 Gateway — DB-to-DB interface 추상화.
 *
 * <p><b>왜 Gateway 인가</b>: 외부 판매 채널 (TOSS, 네이버) 의 HTTP Client 와 달리, 자사 물류 시스템과는
 * DB 차원의 데이터 전달 (interface table polling / DB direct write 등) 이 흔함. Protocol 무관한
 * 추상화이므로 "Client" 가 아닌 Clean Architecture 의 Gateway 명명 채택.</p>
 *
 * <p><b>책임 경계</b>: 자사 물류로의 단일 요청. 멱등성은 호출자가 보장 — 같은 주문에 대해 두 번
 * 호출되어도 같은 결과 (shipmentId 동일) 가 돌아오도록 외부 시스템이 보장 가정.</p>
 *
 * <p>학습 단계 Mock 구현은 service-core 안. 실제 구현 (interface table polling / Kafka 발행 등) 은
 * Phase 1+ 라운드에서 결정.</p>
 */
public interface LogisticsGateway {

    ShipmentResponse request(ShipmentRequest request);
}
