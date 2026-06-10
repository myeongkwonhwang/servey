package io.github.orange2652.partner.channel.saga.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStateRepository;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SagaStateAdvancerTest {

    private static final String SAGA_TYPE = "A1_ORDER_RECEPTION";
    private static final String CORRELATION = "12345";
    private static final String INITIAL_STEP = "STARTED";
    private static final String NEXT_STEP = "UNCONFIRMED_ORDER_SENT";
    private static final String PAYLOAD = "{}";

    @Mock
    private SagaStateRepository sagaStateRepository;

    @InjectMocks
    private SagaStateAdvancer advancer;

    @Test
    @DisplayName("정상 전이 — currentStep 갱신 후 save, true 반환")
    void advanceTransitions() {
        SagaState current = SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, PAYLOAD);
        given(sagaStateRepository.findById(current.sagaId())).willReturn(Optional.of(current));

        boolean advanced = advancer.advance(current.sagaId(), NEXT_STEP);

        assertThat(advanced).isTrue();
        ArgumentCaptor<SagaState> captor = ArgumentCaptor.forClass(SagaState.class);
        verify(sagaStateRepository).save(captor.capture());
        assertThat(captor.getValue().currentStep()).isEqualTo(NEXT_STEP);
        assertThat(captor.getValue().sagaId()).isEqualTo(current.sagaId());
    }

    @Test
    @DisplayName("saga 가 없으면 NoOp, false 반환")
    void notFoundIsNoOp() {
        UUID sagaId = UUID.randomUUID();
        given(sagaStateRepository.findById(sagaId)).willReturn(Optional.empty());

        boolean advanced = advancer.advance(sagaId, NEXT_STEP);

        assertThat(advanced).isFalse();
        verify(sagaStateRepository, never()).save(any());
    }

    @Test
    @DisplayName("이미 같은 step 이면 NoOp, false 반환 (멱등성)")
    void sameStepIsNoOp() {
        SagaState current = SagaState.start(SAGA_TYPE, CORRELATION, NEXT_STEP, PAYLOAD);
        given(sagaStateRepository.findById(current.sagaId())).willReturn(Optional.of(current));

        boolean advanced = advancer.advance(current.sagaId(), NEXT_STEP);

        assertThat(advanced).isFalse();
        verify(sagaStateRepository, never()).save(any());
    }
}
