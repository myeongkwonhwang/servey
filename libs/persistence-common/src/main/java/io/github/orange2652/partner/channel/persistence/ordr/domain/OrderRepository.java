package io.github.orange2652.partner.channel.persistence.ordr.domain;

/**
 * 내부 확정 주문 영속 Port — A1 saga step 3 Pivot.
 *
 * <p>{@code (channel, externalOrderProductId)} UNIQUE 제약 — 중복 INSERT 시
 * {@link org.springframework.dao.DataIntegrityViolationException}. 호출자가 멱등성 결정.</p>
 */
public interface OrderRepository {

    Order save(Order order);
}
