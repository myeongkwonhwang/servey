package io.github.orange2652.partner.channel.adapter.saga.compensate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.client.toss.CancelOrderRequest;
import io.github.orange2652.partner.channel.client.toss.CancelOrderResult;
import io.github.orange2652.partner.channel.client.toss.TossOrderCancelClient;
import io.github.orange2652.partner.channel.common.Channel;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderCompensateCommand;
import io.github.orange2652.partner.channel.kafka.header.KafkaHeaderExtractor;
import io.github.orange2652.partner.channel.persistence.idempotency.IdempotencyGuard;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * step 1 보상 command consume → 외부 cancel → staging UPDATE → reply.
 *
 * <p><b>흐름 (사용자 결정 순서)</b></p>
 * <ol>
 *   <li>{@code saga.order.cmd} 수신 — command-type prefix {@code UNCONFIRMED_ORDER_COMPENSATE} 만</li>
 *   <li>{@code processed_event} 멱등성</li>
 *   <li>{@link UnconfirmedOrderCompensateCommand} parse</li>
 *   <li><b>외부 호출 (Tx 밖)</b>: {@link TossOrderCancelClient#cancel} — PREPARING_PRODUCT → CANCELED_PAYMENT
 *       <br>실패 시 reply FAIL (staging UPDATE 안 함 — 외부가 진실 소스)</li>
 *   <li>(Tx) {@link StagingOrderCancelPersister#cancelIfPresent} — staging.status=CANCELED 또는 NoOp</li>
 *   <li>(Tx 밖) {@link UnconfirmedOrderCompensateReplyPublisher#publishOk}</li>
 * </ol>
 *
 * <p>{@code UNCONFIRMED_ORDER_COMPENSATE} 가 {@code UNCONFIRMED_ORDER} 의 prefix 도 가지므로 기존
 * {@code UnconfirmedOrderCommandConsumer} (prefix {@code UNCONFIRMED_ORDER}) 도 받지만, 그쪽은 정확
 * 매칭 (`UNCONFIRMED_ORDER_REQUEST`) 으로 거부.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class UnconfirmedOrderCompensateCommandConsumer {

    static final String CONSUMER_NAME = "channel-adapter-unconfirmed-order-compensate-cmd";
    static final String COMMAND_TYPE_PREFIX = "UNCONFIRMED_ORDER_COMPENSATE_REQUEST";

    private final IdempotencyGuard idempotencyGuard;
    private final TossOrderCancelClient tossOrderCancelClient;
    private final StagingOrderCancelPersister stagingOrderCancelPersister;
    private final UnconfirmedOrderCompensateReplyPublisher replyPublisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = SagaTopics.ORDER_CMD, groupId = CONSUMER_NAME)
    void onCommand(@Header(name = SagaHeaders.SAGA_ID, required = false) byte[] sagaIdHeader,
                   @Header(name = SagaHeaders.COMMAND_TYPE, required = false) byte[] commandTypeHeader,
                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                   @Header(KafkaHeaders.OFFSET) long offset,
                   @Payload String payload) {

        String commandType = KafkaHeaderExtractor.stringOrEmpty(commandTypeHeader);
        if (!COMMAND_TYPE_PREFIX.equals(commandType)) {
            return;
        }
        UUID sagaId = KafkaHeaderExtractor.uuid(sagaIdHeader).orElse(null);
        if (sagaId == null) {
            log.info("skip — saga-id header missing or invalid UUID partition={} offset={}", partition, offset);
            return;
        }

        String eventId = IdempotencyGuard.eventId(partition, offset);
        if (idempotencyGuard.isAlreadyProcessed(CONSUMER_NAME, eventId)) {
            log.info("skip duplicate eventId={} sagaId={}", eventId, sagaId);
            return;
        }

        UnconfirmedOrderCompensateCommand cmd;
        try {
            cmd = objectMapper.readValue(payload, UnconfirmedOrderCompensateCommand.class);
        } catch (JsonProcessingException e) {
            log.info("compensate command parse failed sagaId={} reason={}", sagaId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "PARSE_FAILED", e.getMessage());
            return;
        }

        if (!Channel.TOSS.code().equals(cmd.channel())) {
            log.info("unsupported channel sagaId={} channel={}", sagaId, cmd.channel());
            replyPublisher.publishFailed(sagaId, "UNSUPPORTED_CHANNEL", "channel=" + cmd.channel());
            return;
        }

        long orderProductId;
        try {
            orderProductId = Long.parseLong(cmd.externalOrderProductId());
        } catch (NumberFormatException e) {
            replyPublisher.publishFailed(sagaId, "INVALID_ORDER_PRODUCT_ID", e.getMessage());
            return;
        }

        CancelOrderResult result;
        try {
            result = tossOrderCancelClient.cancel(orderProductId, CancelOrderRequest.sagaCompensation(cmd.reason()));
        } catch (RuntimeException e) {
            log.info("toss cancel call failed sagaId={} orderProductId={} reason={}",
                    sagaId, orderProductId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "TOSS_CANCEL_FAILED", e.getMessage());
            return;
        }
        if (!result.success()) {
            log.info("toss cancel returned failure sagaId={} errorCode={} errorReason={}",
                    sagaId, result.errorCode(), result.errorReason());
            replyPublisher.publishFailed(sagaId, "TOSS_CANCEL_FAILED",
                    result.errorCode() + ": " + result.errorReason());
            return;
        }

        boolean canceled = stagingOrderCancelPersister.cancelIfPresent(
                CONSUMER_NAME, eventId, cmd.channel(), cmd.externalOrderProductId());

        log.info("compensate completed sagaId={} orderProductId={} stagingCanceled={}",
                sagaId, orderProductId, canceled);
        replyPublisher.publishOk(sagaId);
    }
}
