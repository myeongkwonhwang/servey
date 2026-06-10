package io.github.orange2652.partner.channel.core.saga.confirmedOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.gateway.logistics.LogisticsGateway;
import io.github.orange2652.partner.channel.gateway.logistics.ShipmentRequest;
import io.github.orange2652.partner.channel.gateway.logistics.ShipmentResponse;
import io.github.orange2652.partner.channel.core.saga.confirmedOrder.TossOrderParser.Parsed;
import io.github.orange2652.partner.channel.event.saga.ConfirmedOrderCommand;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import io.github.orange2652.partner.channel.persistence.ordr.domain.Order;
import java.nio.charset.StandardCharsets;
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
 * A1 step 3 — confirmedOrder command consume → 외부 물류 호출 + 내부 주문 INSERT → reply.
 *
 * <p><b>흐름</b></p>
 * <ol>
 *   <li>{@code saga.order.cmd} 수신 — header {@code command-type} 가 {@code CONFIRMED_ORDER} 로 시작하는 것만 처리</li>
 *   <li>{@code processed_event} 중복 처리 차단 (eventId = "{partition}:{offset}")</li>
 *   <li>{@link ConfirmedOrderCommand} JSON parse</li>
 *   <li>{@link TossOrderParser} 로 외부 식별자 추출 (현재 TOSS 만)</li>
 *   <li>(Tx 밖) {@link LogisticsGateway#request} — 외부 물류 호출</li>
 *   <li>(Tx) {@link OrderPersister#persist} — orders INSERT + processed_event INSERT</li>
 *   <li>(Tx 밖) {@link ConfirmedOrderReplyPublisher#publishOk}</li>
 * </ol>
 *
 * <p><b>Pivot</b>: orders INSERT 직후가 Pivot 통과 — 이후 saga 보상 불가, 취소는 별도 saga.</p>
 *
 * <p><b>실패 처리 (학습 단계 단순)</b></p>
 * <ul>
 *   <li>외부 호출 실패: reply FAIL 만. 외부 cancel 미발행</li>
 *   <li>내부 INSERT UNIQUE 충돌: 이미 적재된 것 → OK reply (결과 동일)</li>
 *   <li>내부 INSERT 기타 실패: reply FAIL. 외부 cancel 보상은 다음 라운드</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class ConfirmedOrderCommandConsumer {

    static final String CONSUMER_NAME = "service-core-confirmed-order-cmd";
    static final String COMMAND_TYPE_PREFIX = "CONFIRMED_ORDER";
    static final String CHANNEL_TOSS = "TOSS";

    private final ProcessedEventRepository processedEventRepository;
    private final TossOrderParser tossOrderParser;
    private final LogisticsGateway logisticsGateway;
    private final OrderPersister orderPersister;
    private final ConfirmedOrderReplyPublisher replyPublisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = SagaTopics.ORDER_CMD, groupId = CONSUMER_NAME)
    void onCommand(@Header(name = SagaHeaders.SAGA_ID, required = false) byte[] sagaIdHeader,
                   @Header(name = SagaHeaders.COMMAND_TYPE, required = false) byte[] commandTypeHeader,
                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                   @Header(KafkaHeaders.OFFSET) long offset,
                   @Payload String payload) {

        String commandType = commandTypeHeader == null ? "" : new String(commandTypeHeader, StandardCharsets.UTF_8);
        if (!commandType.startsWith(COMMAND_TYPE_PREFIX)) {
            return;   // 다른 step 의 command — flow only 토픽 구조에서 자기 step 만 처리
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

        ConfirmedOrderCommand command;
        try {
            command = objectMapper.readValue(payload, ConfirmedOrderCommand.class);
        } catch (JsonProcessingException e) {
            log.info("confirmedOrder command parse failed sagaId={} reason={}", sagaId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "PARSE_FAILED", e.getMessage());
            return;
        }

        if (!CHANNEL_TOSS.equals(command.channel())) {
            log.info("unsupported channel sagaId={} channel={}", sagaId, command.channel());
            replyPublisher.publishFailed(sagaId, "UNSUPPORTED_CHANNEL", "channel=" + command.channel());
            return;
        }

        Parsed parsed;
        try {
            parsed = tossOrderParser.parse(command.raw());
        } catch (JsonProcessingException e) {
            log.info("raw parse failed sagaId={} reason={}", sagaId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "RAW_PARSE_FAILED", e.getMessage());
            return;
        }

        ShipmentResponse shipment;
        try {
            shipment = logisticsGateway.request(
                    new ShipmentRequest(command.channel(), parsed.externalOrderProductId()));
        } catch (RuntimeException e) {
            log.info("logistics call failed sagaId={} reason={}", sagaId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "LOGISTICS_FAILED", e.getMessage());
            return;
        }

        Order order = Order.newRecord(
                command.channel(),
                parsed.externalOrderId(),
                parsed.externalOrderProductId(),
                shipment.shipmentId(),
                command.raw());

        Order persisted;
        try {
            persisted = orderPersister.persist(CONSUMER_NAME, eventId, order);
        } catch (DataIntegrityViolationException e) {
            log.info("order already exists sagaId={} channel={} externalOrderProductId={} reason={}",
                    sagaId, order.channel(), order.externalOrderProductId(),
                    e.getMostSpecificCause().getMessage());
            replyPublisher.publishOk(sagaId, null, shipment.shipmentId());
            return;
        }

        log.info("order persisted sagaId={} orderId={} channel={} externalOrderProductId={} shipmentId={}",
                sagaId, persisted.id(), persisted.channel(), persisted.externalOrderProductId(), shipment.shipmentId());
        replyPublisher.publishOk(sagaId, persisted.id(), shipment.shipmentId());
    }
}
