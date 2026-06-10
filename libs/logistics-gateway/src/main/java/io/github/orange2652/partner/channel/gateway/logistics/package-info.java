/**
 * 자사 물류 시스템 Gateway — DB-to-DB interface.
 *
 * <p><b>Channel client 와의 차이</b>: 본 패키지는 외부 판매 채널 (TOSS / 네이버 등) HTTP API 가
 * 아니라 자사 물류 시스템과의 인터페이스. 실제 구현은 DB 직접 write / interface table polling /
 * Kafka 등 protocol 자유. 그래서 "Client" 가 아닌 "Gateway" 명명.</p>
 *
 * <p>학습 단계 Mock 구현은 service-core 안. 실제 구현 (DB connection 등) 은 Phase 1+ 라운드.</p>
 */
package io.github.orange2652.partner.channel.gateway.logistics;
