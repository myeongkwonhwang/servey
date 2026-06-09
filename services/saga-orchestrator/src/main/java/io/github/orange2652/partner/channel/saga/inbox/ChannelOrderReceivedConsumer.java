package io.github.orange2652.partner.channel.saga.inbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
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
 * <p>{@code channel.order.received} consume → {@link SagaState} 생성 → {@link SagaStarter} 위임.</p>
 *
 * <p><b>흐름</b></p>
 * <ol>
 *   <li>Kafka 에서 payload (외부 주문 raw JSON) + key (= correlationKey = orderProductId) 수신</li>
 *   <li>payload 파싱하여 {@code orderProductId} 추출 (key 가 없거나 신뢰 못 하는 경우 대비)</li>
 *   <li>{@link SagaState#start} 로 saga 인스턴스 생성 (status=RUNNING, currentStep=STARTED)</li>
 *   <li>{@link SagaStarter#startIfAbsent} 호출 — UNIQUE 제약으로 중복 시작 방지</li>
 * </ol>
 *
 * <p>다음 step (channel-adapter 의 staging INSERT 명령 발행) 은 별도 라운드.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class ChannelOrderReceivedConsumer {

    static final String TOPIC = "channel.order.received";
    static final String CONSUMER_NAME = "saga-orchestrator";
    static final String SAGA_TYPE = "A1_ORDER_RECEPTION";
    static final String INITIAL_STEP = "STARTED";

    private final SagaStarter sagaStarter;
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

        SagaState state = SagaState.start(SAGA_TYPE, correlationKey, INITIAL_STEP, payload);

        boolean started = sagaStarter.startIfAbsent(state);

        if (started) {
            log.info("saga started sagaId={} sagaType={} correlationKey={}",
                    state.sagaId(), SAGA_TYPE, correlationKey);
        }
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
