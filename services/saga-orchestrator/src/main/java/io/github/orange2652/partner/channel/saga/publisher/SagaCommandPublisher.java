package io.github.orange2652.partner.channel.saga.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.saga.exception.SagaPublishException;
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
 * A1 saga 의 모든 command 발행을 단일 진입점으로 통합. step 별 thin Publisher (UnconfirmedOrder /
 * Validate / ConfirmedOrder / UnconfirmedOrderCompensate) 의 보일러플레이트를 제거.
 *
 * <p>호출자는 (sagaId, sagaStep, commandType, command) 4 인자를 명시한다.
 * sagaType 은 현재 A1 단일 흐름이라 {@link SagaTypes#ORDER_RECEPTION} 고정. B1/B2 추가 시
 * 인자로 승격 검토.</p>
 *
 * <p><b>예외 정책</b>: 직렬화 / 전송 / timeout / interrupt 어떤 실패도 {@link SagaPublishException}
 * 으로 throw. 호출자가 catch 해 saga state stuck 처리 (다음 라운드의 outbox 통합으로 정합성 확보 예정).</p>
 *
 * <p><b>Tx 정책</b>: 외부 (Kafka) WRITE 이므로 호출자 Tx 밖에서 호출.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SagaCommandPublisher {

    static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * @throws SagaPublishException 직렬화 실패 / Kafka send 실패 / timeout / interrupt
     */
    public void publish(UUID sagaId, String sagaStep, String commandType, Object command) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new SagaPublishException(
                    "saga command serialize failed sagaId=%s commandType=%s reason=%s"
                            .formatted(sagaId, commandType, e.getMessage()), e);
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                SagaTopics.ORDER_CMD, null, sagaId.toString(), payload);
        record.headers()
                .add(new RecordHeader(SagaHeaders.SAGA_ID, sagaId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_TYPE, SagaTypes.ORDER_RECEPTION.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_STEP, sagaStep.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.COMMAND_TYPE, commandType.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("saga command published sagaId={} topic={} step={} commandType={}",
                    sagaId, SagaTopics.ORDER_CMD, sagaStep, commandType);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SagaPublishException(
                    "saga command publish interrupted sagaId=%s commandType=%s"
                            .formatted(sagaId, commandType), ie);
        } catch (ExecutionException | TimeoutException e) {
            throw new SagaPublishException(
                    "saga command publish failed sagaId=%s commandType=%s reason=%s"
                            .formatted(sagaId, commandType, e.getMessage()), e);
        }
    }
}
