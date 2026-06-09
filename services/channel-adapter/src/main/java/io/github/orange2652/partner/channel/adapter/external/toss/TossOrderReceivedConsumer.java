package io.github.orange2652.partner.channel.adapter.external.toss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * A1 saga step 1 — channel.order.received consume → staging_order INSERT.
 *
 * 멱등성:
 *   - eventId = "{partition}:{offset}" — Kafka 가 보장하는 unique
 *   - 처리 전 processed_event 조회. 이미 처리됐으면 skip
 *   - 처리 후 staging_order + processed_event 한 Tx 로 INSERT
 *
 * Tx 경계:
 *   - 조회 / 파싱은 Tx 밖
 *   - INSERT 2건만 한 Tx (StagingOrderPersister 가 담당)
 *
 * 채널 식별: 현재 토스 전용 consumer (CHANNEL=TOSS 하드코딩).
 *           향후 채널별 분리 또는 헤더 기반 라우팅으로 진화.
 */
@Slf4j
@Component
@RequiredArgsConstructor
class TossOrderReceivedConsumer {

    static final String TOPIC = "channel.order.received";
    static final String CONSUMER_NAME = "channel-adapter";
    static final String CHANNEL = "TOSS";

    private final ProcessedEventRepository processedEventRepository;
    private final StagingOrderPersister stagingOrderPersister;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = TOPIC, groupId = CONSUMER_NAME)
    void onMessage(@Header(KafkaHeaders.RECEIVED_KEY) String key,
                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                   @Header(KafkaHeaders.OFFSET) long offset,
                   @Payload String payload) {

        String eventId = partition + ":" + offset;

        if (processedEventRepository.exists(CONSUMER_NAME, eventId)) {
            log.info("skip duplicate eventId={} key={}", eventId, key);
            return;
        }

        TossOrderPayload parsed;
        try {
            parsed = objectMapper.readValue(payload, TossOrderPayload.class);
        } catch (JsonProcessingException e) {
            log.info("parse failed eventId={} key={} reason={}", eventId, key, e.getMessage());
            return;
        }

        StagingOrder order = StagingOrder.newRecord(
                CHANNEL,
                String.valueOf(parsed.orderId()),
                String.valueOf(parsed.orderProductId()),
                parsed.orderProductStatus(),
                parsed.orderedAt(),
                payload);

        stagingOrderPersister.persist(CONSUMER_NAME, eventId, order);

        log.info("staging order saved eventId={} channel={} orderProductId={} status={}",
                eventId, CHANNEL, order.externalOrderProductId(), order.externalStatus());
    }
}
