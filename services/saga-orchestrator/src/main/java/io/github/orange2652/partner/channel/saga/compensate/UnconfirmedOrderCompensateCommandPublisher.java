package io.github.orange2652.partner.channel.saga.compensate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderCompensateCommand;
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
 * step 2 (validate) FAIL → step 1 (unconfirmedOrder) 보상 command 발행.
 *
 * <p>Tx 밖에서 호출. 보상은 "반드시 실행되어야 한다" 의 가치가 있으므로 outbox 도입은 다음 라운드 후보
 * ([[project-pcm-external-db-consistency-decision]]).</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UnconfirmedOrderCompensateCommandPublisher {

    static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public boolean publish(UUID sagaId, UnconfirmedOrderCompensateCommand command) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            log.info("compensate command serialize failed sagaId={} reason={}", sagaId, e.getMessage());
            return false;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                SagaTopics.ORDER_CMD, null, sagaId.toString(), payload);
        record.headers()
                .add(new RecordHeader(SagaHeaders.SAGA_ID, sagaId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_TYPE, SagaTypes.ORDER_RECEPTION.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_STEP, SagaSteps.UNCONFIRMED_ORDER_COMPENSATE_SENT.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.COMMAND_TYPE, SagaCommandTypes.UNCONFIRMED_ORDER_COMPENSATE_REQUEST.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("compensate command published sagaId={} channel={} externalOrderProductId={}",
                    sagaId, command.channel(), command.externalOrderProductId());
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("compensate command publish interrupted sagaId={}", sagaId);
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.info("compensate command publish failed sagaId={} reason={}", sagaId, e.getMessage());
            return false;
        }
    }
}
