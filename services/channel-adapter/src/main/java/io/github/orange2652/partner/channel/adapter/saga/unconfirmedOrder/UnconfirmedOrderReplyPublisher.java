package io.github.orange2652.partner.channel.adapter.saga.unconfirmedOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderReply;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * A1 step 1 — unconfirmedOrder reply 발행.
 *
 * <p>DB Tx 밖에서 호출. 발행 실패 시 saga 는 {@code UNCONFIRMED_ORDER_SENT} 에 stuck — 다음 라운드
 * outbox 통합 (channel-adapter 측) 으로 일관성 보장 예정.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class UnconfirmedOrderReplyPublisher {

    static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    void publishOk(UUID sagaId, Long stagingId) {
        publish(sagaId, UnconfirmedOrderReply.ok(stagingId), SagaCommandTypes.UNCONFIRMED_ORDER_REPLY_OK);
    }

    void publishFailed(UUID sagaId, String errorCode, String errorMessage) {
        publish(sagaId, UnconfirmedOrderReply.failed(errorCode, errorMessage),
                SagaCommandTypes.UNCONFIRMED_ORDER_REPLY_FAIL);
    }

    private void publish(UUID sagaId, UnconfirmedOrderReply reply, String commandType) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(reply);
        } catch (JsonProcessingException e) {
            log.info("unconfirmedOrder reply serialize failed sagaId={} reason={}", sagaId, e.getMessage());
            return;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                SagaTopics.ORDER_REPLY, null, sagaId.toString(), payload);
        record.headers()
                .add(new RecordHeader(SagaHeaders.SAGA_ID, sagaId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_TYPE, SagaTypes.ORDER_RECEPTION.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_STEP, SagaSteps.UNCONFIRMED_ORDER_INSERTED.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.COMMAND_TYPE, commandType.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("unconfirmedOrder reply published sagaId={} commandType={}", sagaId, commandType);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("unconfirmedOrder reply publish interrupted sagaId={}", sagaId);
        } catch (ExecutionException | TimeoutException e) {
            log.info("unconfirmedOrder reply publish failed sagaId={} commandType={} reason={}",
                    sagaId, commandType, e.getMessage());
        }
    }
}
