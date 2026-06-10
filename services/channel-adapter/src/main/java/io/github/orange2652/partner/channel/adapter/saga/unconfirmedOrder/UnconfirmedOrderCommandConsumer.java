package io.github.orange2652.partner.channel.adapter.saga.unconfirmedOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.adapter.external.toss.BasicValidator;
import io.github.orange2652.partner.channel.adapter.external.toss.BasicValidator.Verdict;
import io.github.orange2652.partner.channel.adapter.external.toss.TossStagingOrderFactory;
import io.github.orange2652.partner.channel.client.toss.OrderProductStatusChangeResult;
import io.github.orange2652.partner.channel.client.toss.TossOrderStatusClient;
import io.github.orange2652.partner.channel.client.toss.TossOrderStatuses;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderCommand;
import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * A1 step 1 — saga command consume → 기본검증 → 외부 상품준비중 → staging INSERT → reply.
 *
 * <p><b>흐름</b></p>
 * <ol>
 *   <li>{@code saga.order.cmd} 수신 (command-type prefix=UNCONFIRMED_ORDER 만)</li>
 *   <li>{@code processed_event} 중복 처리 차단 (eventId = "{partition}:{offset}")</li>
 *   <li>{@link UnconfirmedOrderCommand} JSON parse</li>
 *   <li><b>기본검증 ({@link BasicValidator})</b> — orderProductStatus=PAID 등 fast fail. 실패 시 reply FAIL (외부 호출 안 함)</li>
 *   <li>채널 raw → 정규화 ({@link TossStagingOrderFactory})</li>
 *   <li><b>외부 호출 (Tx 밖)</b>: {@link TossOrderStatusClient#changeStatus} — PAID → PREPARING_PRODUCT
 *       <br>실패 시 reply FAIL (staging INSERT 안 함 — 외부가 PAID 그대로라 다음 라운드 재시도 가능)</li>
 *   <li>{@link StagingOrderPersister#persist} (Tx) — staging + processed_event 한 Tx</li>
 *   <li>(Tx 밖) {@link UnconfirmedOrderReplyPublisher#publishOk}</li>
 * </ol>
 *
 * <p><b>외부 상태 정합성</b>: 외부 호출 성공 후 staging INSERT 실패 (UNIQUE 충돌) 시 외부는 이미
 * PREPARING_PRODUCT — 결과적 같음이므로 OK reply (멱등성). 외부 호출 자체가 실패면 reply FAIL.</p>
 *
 * <p><b>Tx 경계</b>: 외부 API + DB 쓰기 같은 Tx 금지 (협업 원칙). 외부는 Tx 밖, DB 만 한 Tx.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class UnconfirmedOrderCommandConsumer {

    static final String CONSUMER_NAME = "channel-adapter-unconfirmed-order-cmd";
    static final String EXPECTED_COMMAND_TYPE = "UNCONFIRMED_ORDER_REQUEST";   // 보상 command (UNCONFIRMED_ORDER_COMPENSATE_*) 와 충돌 방지 위해 정확 매칭
    static final String CHANNEL_TOSS = "TOSS";

    private final ProcessedEventRepository processedEventRepository;
    private final BasicValidator basicValidator;
    private final TossOrderStatusClient tossOrderStatusClient;
    private final StagingOrderPersister stagingOrderPersister;
    private final TossStagingOrderFactory tossStagingOrderFactory;
    private final UnconfirmedOrderReplyPublisher replyPublisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = SagaTopics.ORDER_CMD, groupId = CONSUMER_NAME)
    void onCommand(@Header(name = SagaHeaders.SAGA_ID, required = false) byte[] sagaIdHeader,
                   @Header(name = SagaHeaders.COMMAND_TYPE, required = false) byte[] commandTypeHeader,
                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                   @Header(KafkaHeaders.OFFSET) long offset,
                   @Payload String payload) {

        String commandType = commandTypeHeader == null ? "" : new String(commandTypeHeader, StandardCharsets.UTF_8);
        if (!EXPECTED_COMMAND_TYPE.equals(commandType)) {
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

        String eventId = partition + ":" + offset;
        if (processedEventRepository.exists(CONSUMER_NAME, eventId)) {
            log.info("skip duplicate eventId={} sagaId={}", eventId, sagaId);
            return;
        }

        UnconfirmedOrderCommand command;
        try {
            command = objectMapper.readValue(payload, UnconfirmedOrderCommand.class);
        } catch (JsonProcessingException e) {
            log.info("command parse failed sagaId={} reason={}", sagaId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "PARSE_FAILED", e.getMessage());
            return;
        }

        Verdict basic = basicValidator.validate(command.channel(), command.raw());
        if (!basic.valid()) {
            log.info("basic validation failed sagaId={} channel={} errorCode={} errorMessage={}",
                    sagaId, command.channel(), basic.errorCode(), basic.errorMessage());
            replyPublisher.publishFailed(sagaId, basic.errorCode(), basic.errorMessage());
            return;
        }

        StagingOrder order;
        try {
            order = buildOrder(command);
        } catch (UnsupportedChannelException e) {
            log.info("unsupported channel sagaId={} channel={}", sagaId, command.channel());
            replyPublisher.publishFailed(sagaId, "UNSUPPORTED_CHANNEL", e.getMessage());
            return;
        } catch (JsonProcessingException e) {
            log.info("channel raw parse failed sagaId={} channel={} reason={}",
                    sagaId, command.channel(), e.getMessage());
            replyPublisher.publishFailed(sagaId, "CHANNEL_PARSE_FAILED", e.getMessage());
            return;
        }

        long orderProductId = Long.parseLong(order.externalOrderProductId());
        OrderProductStatusChangeResult result;
        try {
            result = tossOrderStatusClient.changeStatus(List.of(orderProductId), TossOrderStatuses.PREPARING_PRODUCT);
        } catch (RuntimeException e) {
            log.info("toss status change failed sagaId={} orderProductId={} reason={}",
                    sagaId, orderProductId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "TOSS_STATUS_CHANGE_FAILED", e.getMessage());
            return;
        }
        if (!result.allSucceeded()) {
            log.info("toss status change partial failure sagaId={} orderProductId={} failedReasons={}",
                    sagaId, orderProductId, result.failedReasons());
            replyPublisher.publishFailed(sagaId, "TOSS_STATUS_CHANGE_FAILED",
                    String.join("; ", result.failedReasons()));
            return;
        }

        StagingOrder persisted;
        try {
            persisted = stagingOrderPersister.persist(CONSUMER_NAME, eventId, order);
        } catch (DataIntegrityViolationException e) {
            log.info("staging already exists sagaId={} channel={} externalOrderProductId={} reason={}",
                    sagaId, order.channel(), order.externalOrderProductId(),
                    e.getMostSpecificCause().getMessage());
            replyPublisher.publishOk(sagaId, null);
            return;
        }

        log.info("staging order saved sagaId={} stagingId={} channel={} externalOrderProductId={} status=PREPARING_PRODUCT",
                sagaId, persisted.id(), persisted.channel(), persisted.externalOrderProductId());
        replyPublisher.publishOk(sagaId, persisted.id());
    }

    private StagingOrder buildOrder(UnconfirmedOrderCommand command) throws JsonProcessingException {
        if (CHANNEL_TOSS.equals(command.channel())) {
            return tossStagingOrderFactory.from(command.raw());
        }
        throw new UnsupportedChannelException("channel=" + command.channel());
    }

    private static final class UnsupportedChannelException extends RuntimeException {
        UnsupportedChannelException(String message) {
            super(message);
        }
    }
}
