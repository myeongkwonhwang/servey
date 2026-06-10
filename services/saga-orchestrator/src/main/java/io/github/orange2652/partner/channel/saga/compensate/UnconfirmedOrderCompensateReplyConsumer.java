package io.github.orange2652.partner.channel.saga.compensate;

import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.saga.state.SagaStateAdvancer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 보상 reply consume → saga state 종결 전이.
 *
 * <ul>
 *   <li>OK → advance {@code UNCONFIRMED_ORDER_COMPENSATED} + status {@code ABORTED}</li>
 *   <li>FAIL → advance {@code UNCONFIRMED_ORDER_COMPENSATE_FAILED} (재시도/수동 개입 — 다음 라운드)</li>
 * </ul>
 *
 * <p>flow only 토픽 구조에서 자기 step 만 처리하기 위해 prefix
 * {@code UNCONFIRMED_ORDER_COMPENSATE} 로 명확히 구분 (기존 {@code UNCONFIRMED_ORDER_REPLY} 와 분리).</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class UnconfirmedOrderCompensateReplyConsumer {

    static final String CONSUMER_NAME = "saga-orchestrator-unconfirmed-order-compensate-reply";
    static final String COMMAND_TYPE_PREFIX = "UNCONFIRMED_ORDER_COMPENSATE_REPLY";

    private final SagaStateAdvancer sagaStateAdvancer;

    @KafkaListener(topics = SagaTopics.ORDER_REPLY, groupId = CONSUMER_NAME)
    void onReply(@Header(name = SagaHeaders.SAGA_ID, required = false) byte[] sagaIdHeader,
                 @Header(name = SagaHeaders.COMMAND_TYPE, required = false) byte[] commandTypeHeader,
                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                 @Header(KafkaHeaders.OFFSET) long offset,
                 @Payload String payload) {

        String commandType = commandTypeHeader == null ? "" : new String(commandTypeHeader, StandardCharsets.UTF_8);
        if (!commandType.startsWith(COMMAND_TYPE_PREFIX)) {
            return;
        }
        if (sagaIdHeader == null) {
            log.info("skip — no saga-id header partition={} offset={}", partition, offset);
            return;
        }
        UUID sagaId;
        try {
            sagaId = UUID.fromString(new String(sagaIdHeader, StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            log.info("skip — saga-id header not UUID partition={} offset={} reason={}",
                    partition, offset, e.getMessage());
            return;
        }

        switch (commandType) {
            case SagaCommandTypes.UNCONFIRMED_ORDER_COMPENSATE_REPLY_OK -> {
                log.info("compensate reply OK sagaId={}", sagaId);
                sagaStateAdvancer.advance(sagaId, SagaSteps.UNCONFIRMED_ORDER_COMPENSATED);
                sagaStateAdvancer.abort(sagaId);
            }
            case SagaCommandTypes.UNCONFIRMED_ORDER_COMPENSATE_REPLY_FAIL -> {
                log.info("compensate reply FAILED sagaId={} payload={}", sagaId, payload);
                sagaStateAdvancer.advance(sagaId, SagaSteps.UNCONFIRMED_ORDER_COMPENSATE_FAILED);
                // 재시도 / 수동 개입은 다음 라운드. saga 는 COMPENSATING 유지
            }
            default ->
                    log.info("skip — unknown commandType sagaId={} commandType={}", sagaId, commandType);
        }
    }
}
