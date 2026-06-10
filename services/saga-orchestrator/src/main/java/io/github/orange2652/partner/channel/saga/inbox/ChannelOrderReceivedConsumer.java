package io.github.orange2652.partner.channel.saga.inbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderCommand;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.saga.state.SagaStateAdvancer;
import io.github.orange2652.partner.channel.saga.unconfirmedOrder.UnconfirmedOrderCommandPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * A1 흐름의 saga 시작 진입점.
 *
 * <p>{@code channel.order.received} consume → {@link SagaState} 생성 → {@link SagaStarter} 위임
 * → {@link UnconfirmedOrderCommandPublisher} 로 step 1 command 발행 → {@link SagaStateAdvancer} 로
 * {@code UNCONFIRMED_ORDER_SENT} 전이.</p>
 *
 * <p><b>흐름</b></p>
 * <ol>
 *   <li>Kafka 에서 payload (외부 raw JSON) + key (= correlationKey = orderProductId) 수신</li>
 *   <li>payload 파싱하여 {@code orderProductId} 추출 (key 없을 때 fallback)</li>
 *   <li>{@link SagaState#start} → {@link SagaStarter#startIfAbsent} (Tx) — UNIQUE 로 중복 시작 차단</li>
 *   <li>(Tx 밖) {@link UnconfirmedOrderCommandPublisher#publish} — Kafka send + ack</li>
 *   <li>(Tx) {@link SagaStateAdvancer#advance} → {@code UNCONFIRMED_ORDER_SENT}</li>
 * </ol>
 *
 * <p>채널: 현재 토스 전용 (channel="TOSS" 고정). 다채널 시 Kafka header 또는 별도 consumer 로 분리.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class ChannelOrderReceivedConsumer {

    static final String TOPIC = "channel.order.received";
    static final String CONSUMER_NAME = "saga-orchestrator";
    static final String CHANNEL = "TOSS";

    private final SagaStarter sagaStarter;
    private final UnconfirmedOrderCommandPublisher unconfirmedOrderCommandPublisher;
    private final SagaStateAdvancer sagaStateAdvancer;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = TOPIC, groupId = CONSUMER_NAME)
    void onMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                   @Header(KafkaHeaders.OFFSET) long offset,
                   @Payload String payload) {

        String correlationKey = extractCorrelationKey(key, payload);
        if (correlationKey == null) {
            log.info("skip — no correlationKey partition={} offset={} key={}", partition, offset, key);
            return;
        }

        SagaState state = SagaState.start(SagaTypes.ORDER_RECEPTION, correlationKey, SagaSteps.STARTED, payload);

        boolean started = sagaStarter.startIfAbsent(state);
        if (!started) {
            return;
        }
        log.info("saga started sagaId={} sagaType={} correlationKey={}",
                state.sagaId(), SagaTypes.ORDER_RECEPTION, correlationKey);

        UnconfirmedOrderCommand command = new UnconfirmedOrderCommand(CHANNEL, payload);
        boolean published = unconfirmedOrderCommandPublisher.publish(state.sagaId(), command);
        if (!published) {
            log.info("unconfirmedOrder command publish failed — saga stays at STARTED sagaId={}", state.sagaId());
            return;
        }

        sagaStateAdvancer.advance(state.sagaId(), SagaSteps.UNCONFIRMED_ORDER_SENT);
    }

    /**
     * correlationKey 결정 — key 가 있으면 그대로, 없으면 payload 의 {@code orderProductId} fallback.
     * 둘 다 없으면 {@code null}.
     */
    private String extractCorrelationKey(String key, String payload) {
        if (key != null && !key.isBlank()) {
            return key;
        }
        try {
            ObjectNode node = (ObjectNode) objectMapper.readTree(payload);
            if (node.hasNonNull("orderProductId")) {
                return node.get("orderProductId").asText();
            }
        } catch (JsonProcessingException e) {
            log.info("payload parse failed reason={}", e.getMessage());
        }
        return null;
    }
}
