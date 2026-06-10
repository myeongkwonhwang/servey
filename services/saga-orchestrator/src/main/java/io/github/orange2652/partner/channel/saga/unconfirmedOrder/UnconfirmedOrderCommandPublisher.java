package io.github.orange2652.partner.channel.saga.unconfirmedOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderCommand;
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
 * A1 step 1 — unconfirmedOrder command 발행.
 *
 * <p>Tx 밖에서 호출. saga state INSERT (=={@code SagaStarter}) 가 끝난 직후, ack 대기 후
 * {@link io.github.orange2652.partner.channel.saga.state.SagaStateAdvancer} 로
 * {@code UNCONFIRMED_ORDER_SENT} 전이.</p>
 *
 * <p><b>한계 (학습 단계)</b>: 발행 실패 시 saga 는 {@code STARTED} 에 stuck. 다음 라운드에서
 * outbox 통합으로 saga state INSERT 와 같은 Tx 에 묶을 예정.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UnconfirmedOrderCommandPublisher {

    static final long SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * @return ack 까지 정상 발행됐으면 true
     */
    public boolean publish(UUID sagaId, UnconfirmedOrderCommand command) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            log.info("unconfirmedOrder command serialize failed sagaId={} reason={}", sagaId, e.getMessage());
            return false;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                SagaTopics.ORDER_CMD, null, sagaId.toString(), payload);
        record.headers()
                .add(new RecordHeader(SagaHeaders.SAGA_ID, sagaId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_TYPE, SagaTypes.ORDER_RECEPTION.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.SAGA_STEP, SagaSteps.UNCONFIRMED_ORDER_SENT.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(SagaHeaders.COMMAND_TYPE, SagaCommandTypes.UNCONFIRMED_ORDER_REQUEST.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("unconfirmedOrder command published sagaId={} topic={} channel={}",
                    sagaId, SagaTopics.ORDER_CMD, command.channel());
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("unconfirmedOrder command publish interrupted sagaId={}", sagaId);
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.info("unconfirmedOrder command publish failed sagaId={} reason={}", sagaId, e.getMessage());
            return false;
        }
    }
}
