package io.github.orange2652.partner.channel.event.saga;

/**
 * Saga command/reply Kafka topic 상수 — flow only 분리.
 *
 * <p>네이밍 규칙: {@code saga.{flow}.cmd} / {@code saga.{flow}.reply}.
 * step / channel 차원은 토픽 분리 X — header {@code command-type} / payload {@code channel} 로 분기.</p>
 *
 * <p><b>설계 결정</b> ([[project-pcm-flow-only-topics]]):</p>
 * <ul>
 *   <li>운영 단순 우선 (~4 토픽) — Kafka cluster / 모니터링 부담 ↓</li>
 *   <li>비용: 모든 consumer 가 자기 토픽의 모든 메시지 수신 후 header filter</li>
 *   <li>step 별 lag 모니터링 손실 — saga_state.currentStep 분포로 대체</li>
 *   <li>다음 escalation: partition key = channel 또는 (필요 시) step 차원 토픽 분리</li>
 * </ul>
 *
 * <p>도메인 이벤트 (saga 트리거) 는 {@code {domain}.{event}} — 예: {@code channel.order.received}.</p>
 */
public final class SagaTopics {

    public static final String ORDER_CMD = "saga.order.cmd";
    public static final String ORDER_REPLY = "saga.order.reply";

    private SagaTopics() {
    }
}
