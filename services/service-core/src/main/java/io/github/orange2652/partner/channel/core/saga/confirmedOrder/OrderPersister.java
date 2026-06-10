package io.github.orange2652.partner.channel.core.saga.confirmedOrder;

import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEvent;
import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import io.github.orange2652.partner.channel.persistence.ordr.domain.Order;
import io.github.orange2652.partner.channel.persistence.ordr.domain.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * orders INSERT + processed_event INSERT 한 Tx — Pivot 통과 시점.
 *
 * <p>별도 @Component 로 분리 (consumer self-invocation 회피).</p>
 *
 * <p>중복 INSERT: orders UNIQUE / processed_event PK 가 차단 — 호출자가
 * {@link org.springframework.dao.DataIntegrityViolationException} catch 후 OK reply 발행
 * (이미 처리된 결과적 같음).</p>
 */
@Component
@RequiredArgsConstructor
class OrderPersister {

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;

    @Transactional
    public Order persist(String consumerName, String eventId, Order order) {
        Order saved = orderRepository.save(order);
        processedEventRepository.save(ProcessedEvent.newRecord(consumerName, eventId));
        return saved;
    }
}
