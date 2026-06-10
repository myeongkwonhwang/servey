package io.github.orange2652.partner.channel.adapter.saga.compensate;

import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEvent;
import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrder;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrderRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 보상: staging UPDATE (status=CANCELED) + processed_event INSERT 한 Tx.
 *
 * <p>self-invocation 회피 위해 별도 @Component.</p>
 *
 * <p>이미 CANCELED 인 경우 NoOp (멱등성).</p>
 */
@Component
@RequiredArgsConstructor
class StagingOrderCancelPersister {

    private final StagingOrderRepository stagingOrderRepository;
    private final ProcessedEventRepository processedEventRepository;

    /**
     * @return 실제로 CANCELED 전이된 staging 존재 시 true. staging 미존재 / 이미 CANCELED 면 false
     */
    @Transactional
    public boolean cancelIfPresent(String consumerName, String eventId,
                                   String channel, String externalOrderProductId) {
        Optional<StagingOrder> found = stagingOrderRepository
                .findByChannelAndExternalOrderProductId(channel, externalOrderProductId);
        if (found.isEmpty()) {
            processedEventRepository.save(ProcessedEvent.newRecord(consumerName, eventId));
            return false;
        }
        StagingOrder current = found.get();
        if (current.isCanceled()) {
            processedEventRepository.save(ProcessedEvent.newRecord(consumerName, eventId));
            return false;
        }
        stagingOrderRepository.save(current.markCanceled());
        processedEventRepository.save(ProcessedEvent.newRecord(consumerName, eventId));
        return true;
    }
}
