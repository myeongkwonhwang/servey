package io.github.orange2652.partner.channel.saga.validate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.ConfirmedOrderCommand;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.UnconfirmedOrderCompensateCommand;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStateRepository;
import io.github.orange2652.partner.channel.saga.compensate.UnconfirmedOrderCompensateCommandPublisher;
import io.github.orange2652.partner.channel.saga.confirmedOrder.ConfirmedOrderCommandPublisher;
import io.github.orange2652.partner.channel.saga.state.SagaStateAdvancer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * A1 step 2 → step 3 — validate reply consume → saga state advance + confirmedOrder command 발행.
 *
 * <p><b>흐름 (OK reply 인 경우)</b></p>
 * <ol>
 *   <li>saga state advance to {@code VALIDATED}</li>
 *   <li>saga state 조회 → payload (외부 raw) 가져옴</li>
 *   <li>{@link ConfirmedOrderCommand} 생성 — channel + raw</li>
 *   <li>{@link ConfirmedOrderCommandPublisher#publish} (Tx 밖)</li>
 *   <li>saga state advance to {@code CONFIRMED_ORDER_SENT}</li>
 * </ol>
 *
 * <p>FAIL reply 는 {@code VALIDATE_FAILED} 로 advance 만. 보상 (staging DELETE) 은 다음 라운드.</p>
 *
 * <p>채널: 현재 토스 전용 ({@code CHANNEL="TOSS"} 하드코딩) — saga state 에 채널 컬럼 없음.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class ValidateReplyConsumer {

    static final String CONSUMER_NAME = "saga-orchestrator-validate-reply";
    static final String COMMAND_TYPE_PREFIX = "VALIDATE";
    static final String CHANNEL = "TOSS";

    private final SagaStateAdvancer sagaStateAdvancer;
    private final SagaStateRepository sagaStateRepository;
    private final ConfirmedOrderCommandPublisher confirmedOrderCommandPublisher;
    private final UnconfirmedOrderCompensateCommandPublisher compensateCommandPublisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = SagaTopics.ORDER_REPLY, groupId = CONSUMER_NAME)
    void onReply(@Header(name = SagaHeaders.SAGA_ID, required = false) byte[] sagaIdHeader,
                 @Header(name = SagaHeaders.COMMAND_TYPE, required = false) byte[] commandTypeHeader,
                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                 @Header(KafkaHeaders.OFFSET) long offset,
                 @Payload String payload) {

        String commandType = commandTypeHeader == null ? "" : new String(commandTypeHeader, StandardCharsets.UTF_8);
        if (!commandType.startsWith(COMMAND_TYPE_PREFIX)) {
            return;   // 다른 step 의 reply — flow only 토픽 구조에서 자기 step 만 처리
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

        switch (commandType) {
            case SagaCommandTypes.VALIDATE_REPLY_OK -> handleOk(sagaId);
            case SagaCommandTypes.VALIDATE_REPLY_FAIL -> handleFail(sagaId, payload);
            default ->
                    log.info("skip — unknown commandType sagaId={} commandType={}", sagaId, commandType);
        }
    }

    private void handleFail(UUID sagaId, String replyPayload) {
        log.info("validate reply FAILED sagaId={} payload={}", sagaId, replyPayload);
        sagaStateAdvancer.advance(sagaId, SagaSteps.VALIDATE_FAILED);
        sagaStateAdvancer.compensate(sagaId);

        Optional<SagaState> found = sagaStateRepository.findById(sagaId);
        if (found.isEmpty()) {
            log.info("skip compensate publish — saga not found sagaId={}", sagaId);
            return;
        }

        String externalOrderProductId = extractExternalOrderProductId(found.get().payload());
        if (externalOrderProductId == null) {
            log.info("skip compensate publish — cannot extract externalOrderProductId sagaId={}", sagaId);
            return;
        }

        UnconfirmedOrderCompensateCommand cmd = new UnconfirmedOrderCompensateCommand(
                CHANNEL, externalOrderProductId, "validate FAILED");
        boolean published = compensateCommandPublisher.publish(sagaId, cmd);
        if (!published) {
            log.info("compensate command publish failed — saga stays COMPENSATING sagaId={}", sagaId);
            return;
        }
        sagaStateAdvancer.advance(sagaId, SagaSteps.UNCONFIRMED_ORDER_COMPENSATE_SENT);
    }

    /**
     * saga state.payload (외부 raw) 에서 orderProductId 추출. TOSS 전용.
     */
    private String extractExternalOrderProductId(String raw) {
        try {
            JsonNode node = objectMapper.readTree(raw);
            if (node.hasNonNull("orderProductId")) {
                return node.get("orderProductId").asText();
            }
        } catch (JsonProcessingException e) {
            log.info("payload parse failed reason={}", e.getMessage());
        }
        return null;
    }

    private void handleOk(UUID sagaId) {
        sagaStateAdvancer.advance(sagaId, SagaSteps.VALIDATED);

        Optional<SagaState> found = sagaStateRepository.findById(sagaId);
        if (found.isEmpty()) {
            log.info("skip confirmedOrder publish — saga not found sagaId={}", sagaId);
            return;
        }

        ConfirmedOrderCommand command = new ConfirmedOrderCommand(CHANNEL, found.get().payload());
        boolean published = confirmedOrderCommandPublisher.publish(sagaId, command);
        if (!published) {
            log.info("confirmedOrder command publish failed — saga stays at VALIDATED sagaId={}", sagaId);
            return;
        }

        sagaStateAdvancer.advance(sagaId, SagaSteps.CONFIRMED_ORDER_SENT);
    }
}
