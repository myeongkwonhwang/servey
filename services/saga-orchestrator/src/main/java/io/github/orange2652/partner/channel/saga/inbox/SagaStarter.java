package io.github.orange2652.partner.channel.saga.inbox;

import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Saga 시작 application service.
 *
 * <p>외부에서 받은 메시지 한 건 → {@code saga_state} INSERT 한 Tx.</p>
 *
 * <p><b>멱등성</b>: {@code (sagaType, correlationKey)} UNIQUE 제약 위반 시 catch → skip.
 * 별도 {@code processed_event} 테이블 없이 DB 제약만으로 보장. {@code at-least-once} delivery
 * (Kafka) 환경에서 같은 메시지가 두 번 들어와도 saga 는 한 번만 시작.</p>
 *
 * <p><b>Tx 경계</b>: 별도 @Component 로 분리해 {@code @Transactional} 의 proxy 가 보장되게 함
 * (consumer 와 같은 클래스에 두면 self-invocation 으로 우회 위험).</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
class SagaStarter {

    private final SagaStateRepository sagaStateRepository;

    /**
     * 새 saga 시작 — 이미 시작된 경우 skip.
     *
     * @param state 시작할 saga 상태 (status=RUNNING, 신규 sagaId)
     * @return 실제로 INSERT 됐으면 {@code true}, 이미 존재해서 skip 했으면 {@code false}
     */
    @Transactional
    public boolean startIfAbsent(SagaState state) {
        try {
            sagaStateRepository.save(state);
            return true;
        } catch (DataIntegrityViolationException e) {
            log.info("saga already started sagaType={} correlationKey={} reason={}",
                    state.sagaType(), state.correlationKey(), e.getMostSpecificCause().getMessage());
            return false;
        }
    }
}
