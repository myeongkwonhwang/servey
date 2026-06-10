package io.github.orange2652.partner.channel.core.saga.validate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.core.saga.validate.Validator.Verdict;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.ValidateCommand;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * A1 step 2 — validate command consume → 판매가능 검증 → reply 발행.
 *
 * <p><b>흐름</b></p>
 * <ol>
 *   <li>{@code saga.order.cmd.validate} 수신 (header: saga-id 필수)</li>
 *   <li>{@link ValidateCommand} JSON parse</li>
 *   <li>{@link Validator#validate} — 채널별 룰</li>
 *   <li>{@link ValidateReplyPublisher#publishOk} / {@link ValidateReplyPublisher#publishFailed}</li>
 * </ol>
 *
 * <p>read-only step — DB 쓰기 없음. {@code processed_event} 멱등성 미적용 (검증 재수행은
 * 부작용 없음). saga-orchestrator 의 advance 가 NoOp 으로 보장.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class ValidateCommandConsumer {

    static final String CONSUMER_NAME = "service-core-validate-cmd";
    static final String COMMAND_TYPE_PREFIX = "VALIDATE";

    private final Validator validator;
    private final ValidateReplyPublisher replyPublisher;
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

        ValidateCommand command;
        try {
            command = objectMapper.readValue(payload, ValidateCommand.class);
        } catch (JsonProcessingException e) {
            log.info("validate command parse failed sagaId={} reason={}", sagaId, e.getMessage());
            replyPublisher.publishFailed(sagaId, "PARSE_FAILED", e.getMessage());
            return;
        }

        Verdict verdict = validator.validate(command.channel(), command.raw());
        if (verdict.sellable()) {
            log.info("validate OK sagaId={} channel={}", sagaId, command.channel());
            replyPublisher.publishOk(sagaId);
        } else {
            log.info("validate FAILED sagaId={} channel={} errorCode={} errorMessage={}",
                    sagaId, command.channel(), verdict.errorCode(), verdict.errorMessage());
            replyPublisher.publishFailed(sagaId, verdict.errorCode(), verdict.errorMessage());
        }
    }
}
