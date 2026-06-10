package io.github.orange2652.partner.channel.saga.confirmedOrder;

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
 * A1 step 3 — confirmedOrder reply consume → saga state advance.
 *
 * <ul>
 *   <li>OK → {@code CONFIRMED_ORDER_PERSISTED} (= Pivot 통과)</li>
 *   <li>FAIL → {@code CONFIRMED_ORDER_FAILED}</li>
 * </ul>
 *
 * <p>Pivot 통과 후 saga 는 종결 단계로. (현재 라운드 happy path 만 — COMPLETED 전이는 메모리만)</p>
 *
 * <p>FAIL 처리 (외부 호출 후 내부 INSERT 실패) 시 외부 cancel 보상은 다음 라운드.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class ConfirmedOrderReplyConsumer {

    static final String CONSUMER_NAME = "saga-orchestrator-confirmed-order-reply";
    static final String COMMAND_TYPE_PREFIX = "CONFIRMED_ORDER";

    private final SagaStateAdvancer sagaStateAdvancer;

    @KafkaListener(topics = SagaTopics.ORDER_REPLY, groupId = CONSUMER_NAME)
    void onReply(@Header(name = SagaHeaders.SAGA_ID, required = false) byte[] sagaIdHeader,
                 @Header(name = SagaHeaders.COMMAND_TYPE, required = false) byte[] commandTypeHeader,
                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                 @Header(KafkaHeaders.OFFSET) long offset,
                 @Payload String payload) {

        String commandType = commandTypeHeader == null ? "" : new String(commandTypeHeader, StandardCharsets.UTF_8);
        if (!commandType.startsWith(COMMAND_TYPE_PREFIX)) {
            return;   // 다른 step 의 reply — flow only 토픽 구조에서 자기 step 만 처리
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
            case SagaCommandTypes.CONFIRMED_ORDER_REPLY_OK -> {
                log.info("confirmedOrder reply OK sagaId={} payload={}", sagaId, payload);
                sagaStateAdvancer.advance(sagaId, SagaSteps.CONFIRMED_ORDER_PERSISTED);
            }
            case SagaCommandTypes.CONFIRMED_ORDER_REPLY_FAIL -> {
                log.info("confirmedOrder reply FAILED sagaId={} payload={}", sagaId, payload);
                sagaStateAdvancer.advance(sagaId, SagaSteps.CONFIRMED_ORDER_FAILED);
            }
            default ->
                    log.info("skip — unknown commandType sagaId={} commandType={}", sagaId, commandType);
        }
    }
}
