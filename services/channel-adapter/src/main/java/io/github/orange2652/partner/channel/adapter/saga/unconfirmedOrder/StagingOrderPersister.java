package io.github.orange2652.partner.channel.adapter.saga.unconfirmedOrder;

import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEvent;
import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrder;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * staging_order INSERT + processed_event INSERT 를 한 Tx 로 묶음.
 *
 * <p>별도 @Component 로 분리한 이유: consumer 와 같은 클래스의 @Transactional 메서드를
 * self-invocation 하면 proxy 우회로 Tx 가 적용되지 않음.</p>
 *
 * <p>중복 INSERT 는 staging UNIQUE / processed_event PK 가 차단 → 호출자가
 * {@link org.springframework.dao.DataIntegrityViolationException} 을 catch 하여 멱등성 결정.</p>
 */
@Component
@RequiredArgsConstructor
class StagingOrderPersister {

    private final StagingOrderRepository stagingOrderRepository;
    private final ProcessedEventRepository processedEventRepository;

    @Transactional
    public StagingOrder persist(String consumerName, String eventId, StagingOrder order) {
        StagingOrder saved = stagingOrderRepository.save(order);
        processedEventRepository.save(ProcessedEvent.newRecord(consumerName, eventId));
        return saved;
    }
}
