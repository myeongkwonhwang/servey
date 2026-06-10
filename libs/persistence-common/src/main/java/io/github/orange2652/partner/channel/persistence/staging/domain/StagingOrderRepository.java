package io.github.orange2652.partner.channel.persistence.staging.domain;

import java.util.Optional;

/**
 * 비정규 주문 적재 Port — A1 saga step 1 결과 영속.
 *
 * <p>{@code (channel, externalOrderProductId)} UNIQUE 제약이 걸려 있으므로 같은 외부 주문이
 * 두 번 INSERT 되면 {@link org.springframework.dao.DataIntegrityViolationException} 발생.
 * 호출자가 멱등성을 별도로 보장하지 않으면 catch + skip 으로 처리한다.</p>
 */
public interface StagingOrderRepository {

    StagingOrder save(StagingOrder order);

    /**
     * 보상 흐름에서 사용 — 외부 식별자로 조회 후 markCanceled → save.
     */
    Optional<StagingOrder> findByChannelAndExternalOrderProductId(String channel, String externalOrderProductId);
}
