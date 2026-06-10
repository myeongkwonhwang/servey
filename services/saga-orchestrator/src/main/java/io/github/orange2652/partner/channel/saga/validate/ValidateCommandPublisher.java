package io.github.orange2652.partner.channel.saga.validate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.event.saga.ValidateCommand;
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
 * A1 step 2 — validate command 발행.
 *
 * <p>Tx 밖에서 호출. step 1 reply 처리 후 호출 → ack 대기 후 saga state advance to
 * {@code VALIDATE_SENT}.</p>
 *
 * <p><b>한계 (학습 단계)</b>: 발행 실패 시 saga 는 {@code UNCONFIRMED_ORDER_INSERTED} 에 stuck.
 * outbox 통합은 다음 라운드.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ValidateCommandPublisher {

    static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public boolean publish(UUID sagaId, ValidateCommand command) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            log.info("validate command serialize failed sagaId={} reason={}", sagaId, e.getMessage());
            return false;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                SagaTopics.ORDER_CMD, null, sagaId.toString(), payload);
        record.headers()
                .add(new RecordHeader(SagaHeaders.SAGA_ID, sagaId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_TYPE, SagaTypes.ORDER_RECEPTION.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_STEP, SagaSteps.VALIDATE_SENT.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.COMMAND_TYPE, SagaCommandTypes.VALIDATE_REQUEST.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("validate command published sagaId={} topic={} channel={}",
                    sagaId, SagaTopics.ORDER_CMD, command.channel());
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("validate command publish interrupted sagaId={}", sagaId);
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.info("validate command publish failed sagaId={} reason={}", sagaId, e.getMessage());
            return false;
        }
    }
}
