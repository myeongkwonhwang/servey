package io.github.orange2652.partner.channel.saga.unconfirmedOrder;

import io.github.orange2652.partner.channel.common.Channel;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.ValidateCommand;
import io.github.orange2652.partner.channel.kafka.header.KafkaHeaderExtractor;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStateRepository;
import io.github.orange2652.partner.channel.saga.exception.SagaPublishException;
import io.github.orange2652.partner.channel.saga.publisher.SagaCommandPublisher;
import io.github.orange2652.partner.channel.saga.state.SagaStateAdvancer;
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
 * A1 step 1 → step 2 — unconfirmedOrder reply consume → saga state advance + validate command 발행.
 *
 * <p><b>흐름 (OK reply 인 경우)</b></p>
 * <ol>
 *   <li>saga state advance to {@code UNCONFIRMED_ORDER_INSERTED}</li>
 *   <li>saga state 조회 → payload (외부 raw) 가져옴</li>
 *   <li>{@link ValidateCommand} 생성 — channel + raw</li>
 *   <li>{@link SagaCommandPublisher#publish} (Tx 밖). 실패 시 {@link SagaPublishException}</li>
 *   <li>saga state advance to {@code VALIDATE_SENT}</li>
 * </ol>
 *
 * <p>FAIL reply 는 일단 로깅만 (보상 흐름은 다음 라운드).</p>
 *
 * <p>채널: 현재 토스 전용 ({@code CHANNEL="TOSS"} 하드코딩). 다채널 시 saga state 에 채널 컬럼 추가
 * 또는 step 1 reply 에 channel 포함으로 확장.</p>
 *
 * <p>멱등성: {@link SagaStateAdvancer#advance} 가 currentStep 비교로 NoOp.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class UnconfirmedOrderReplyConsumer {

    static final String CONSUMER_NAME = "saga-orchestrator-unconfirmed-order-reply";
    static final String COMMAND_TYPE_PREFIX = "UNCONFIRMED_ORDER_REPLY";   // 보상 reply (UNCONFIRMED_ORDER_COMPENSATE_REPLY_*) 와 충돌 방지

    private final SagaStateAdvancer sagaStateAdvancer;
    private final SagaStateRepository sagaStateRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    @KafkaListener(topics = SagaTopics.ORDER_REPLY, groupId = CONSUMER_NAME)
    void onReply(@Header(name = SagaHeaders.SAGA_ID, required = false) byte[] sagaIdHeader,
                 @Header(name = SagaHeaders.COMMAND_TYPE, required = false) byte[] commandTypeHeader,
                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                 @Header(KafkaHeaders.OFFSET) long offset,
                 @Payload String payload) {

        String commandType = KafkaHeaderExtractor.stringOrEmpty(commandTypeHeader);
        if (!commandType.startsWith(COMMAND_TYPE_PREFIX)) {
            return;   // 다른 step 의 reply — flow only 토픽 구조에서 자기 step 만 처리
        }
        UUID sagaId = KafkaHeaderExtractor.uuid(sagaIdHeader).orElse(null);
        if (sagaId == null) {
            log.info("skip — saga-id header missing or invalid UUID partition={} offset={}", partition, offset);
            return;
        }

        switch (commandType) {
            case SagaCommandTypes.UNCONFIRMED_ORDER_REPLY_OK -> handleOk(sagaId);
            case SagaCommandTypes.UNCONFIRMED_ORDER_REPLY_FAIL ->
                    log.info("unconfirmedOrder reply FAILED sagaId={} payload={}", sagaId, payload);
            default ->
                    log.info("skip — unknown commandType sagaId={} commandType={}", sagaId, commandType);
        }
    }

    private void handleOk(UUID sagaId) {
        sagaStateAdvancer.advance(sagaId, SagaSteps.UNCONFIRMED_ORDER_INSERTED);

        Optional<SagaState> found = sagaStateRepository.findById(sagaId);
        if (found.isEmpty()) {
            log.info("skip validate publish — saga not found sagaId={}", sagaId);
            return;
        }

        ValidateCommand command = new ValidateCommand(Channel.TOSS.code(), found.orElseThrow().payload());
        try {
            sagaCommandPublisher.publish(sagaId, SagaSteps.VALIDATE_SENT,
                    SagaCommandTypes.VALIDATE_REQUEST, command);
        } catch (SagaPublishException e) {
            log.info("validate command publish failed — saga stays at UNCONFIRMED_ORDER_INSERTED sagaId={} reason={}",
                    sagaId, e.getMessage());
            return;
        }

        sagaStateAdvancer.advance(sagaId, SagaSteps.VALIDATE_SENT);
    }
}
