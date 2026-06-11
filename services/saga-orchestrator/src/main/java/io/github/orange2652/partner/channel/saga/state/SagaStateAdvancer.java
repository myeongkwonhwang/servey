package io.github.orange2652.partner.channel.saga.state;

import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStateRepository;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStatus;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Saga state lifecycle 전이 application service — 각 메서드 한 Tx.
 *
 * <p>currentStep 전이 ({@link #advance}) + status 전이 ({@link #compensate} / {@link #abort} /
 * {@link #complete}). 멱등성 — 이미 같은 상태면 NoOp.</p>
 *
 * <p>별도 @Component 로 분리해 {@code @Transactional} proxy 가 보장되도록.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SagaStateAdvancer {

    private final SagaStateRepository sagaStateRepository;

    /**
     * currentStep 전이. saga 없거나 이미 같은 step 이면 NoOp.
     */
    @Transactional
    public boolean advance(UUID sagaId, String nextStep) {
        Optional<SagaState> found = sagaStateRepository.findById(sagaId);
        if (found.isEmpty()) {
            log.info("saga advance skipped — not found sagaId={} nextStep={}", sagaId, nextStep);
            return false;
        }
        SagaState current = found.orElseThrow();
        if (nextStep.equals(current.currentStep())) {
            log.info("saga advance skipped — already at step sagaId={} step={}", sagaId, nextStep);
            return false;
        }
        sagaStateRepository.save(current.advance(nextStep));
        log.info("saga advanced sagaId={} prevStep={} nextStep={}", sagaId, current.currentStep(), nextStep);
        return true;
    }

    /**
     * status = COMPENSATING 전이 — 보상 흐름 시작.
     */
    @Transactional
    public boolean compensate(UUID sagaId) {
        return transitStatus(sagaId, SagaStatus.COMPENSATING, SagaState::compensate);
    }

    /**
     * status = ABORTED 전이 — 보상 완료 또는 비복구 실패.
     */
    @Transactional
    public boolean abort(UUID sagaId) {
        return transitStatus(sagaId, SagaStatus.ABORTED, SagaState::abort);
    }

    /**
     * status = COMPLETED 전이 — saga 정상 종료.
     */
    @Transactional
    public boolean complete(UUID sagaId) {
        return transitStatus(sagaId, SagaStatus.COMPLETED, SagaState::complete);
    }

    private boolean transitStatus(UUID sagaId, SagaStatus targetStatus,
                                  java.util.function.UnaryOperator<SagaState> transition) {
        Optional<SagaState> found = sagaStateRepository.findById(sagaId);
        if (found.isEmpty()) {
            log.info("saga status transit skipped — not found sagaId={} target={}", sagaId, targetStatus);
            return false;
        }
        SagaState current = found.orElseThrow();
        if (targetStatus == current.status()) {
            log.info("saga status transit skipped — already at status sagaId={} status={}",
                    sagaId, targetStatus);
            return false;
        }
        sagaStateRepository.save(transition.apply(current));
        log.info("saga status transit sagaId={} prev={} next={}", sagaId, current.status(), targetStatus);
        return true;
    }
}
