/**
 * 내부 도메인 상태 polling.
 * 도메인 schema 의 hand-off 조건 / 재시도 잔재 / 시간 기반 트리거 등을 폴링해 outbox 적재.
 * 도메인 결합 인정 — Phase 1 에서 도메인별 batch 로 분리 가능.
 */
package io.github.orange2652.partner.channel.batch.internal;
