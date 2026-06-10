package io.github.orange2652.partner.channel.adapter.saga.compensate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderCompensateReply;
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
 * 보상 reply 발행 (Tx 밖).
 */
@Slf4j
@Component
@RequiredArgsConstructor
class UnconfirmedOrderCompensateReplyPublisher {

    static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    void publishOk(UUID sagaId) {
        publish(sagaId, UnconfirmedOrderCompensateReply.ok(),
                SagaCommandTypes.UNCONFIRMED_ORDER_COMPENSATE_REPLY_OK, SagaSteps.UNCONFIRMED_ORDER_COMPENSATED);
    }

    void publishFailed(UUID sagaId, String errorCode, String errorMessage) {
        publish(sagaId, UnconfirmedOrderCompensateReply.failed(errorCode, errorMessage),
                SagaCommandTypes.UNCONFIRMED_ORDER_COMPENSATE_REPLY_FAIL, SagaSteps.UNCONFIRMED_ORDER_COMPENSATE_FAILED);
    }

    private void publish(UUID sagaId, UnconfirmedOrderCompensateReply reply, String commandType, String sagaStep) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(reply);
        } catch (JsonProcessingException e) {
            log.info("compensate reply serialize failed sagaId={} reason={}", sagaId, e.getMessage());
            return;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                SagaTopics.ORDER_REPLY, null, sagaId.toString(), payload);
        record.headers()
                .add(new RecordHeader(SagaHeaders.SAGA_ID, sagaId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_TYPE, SagaTypes.ORDER_RECEPTION.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_STEP, sagaStep.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.COMMAND_TYPE, commandType.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("compensate reply published sagaId={} commandType={}", sagaId, commandType);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("compensate reply publish interrupted sagaId={}", sagaId);
        } catch (ExecutionException | TimeoutException e) {
            log.info("compensate reply publish failed sagaId={} commandType={} reason={}",
                    sagaId, commandType, e.getMessage());
        }
    }
}
