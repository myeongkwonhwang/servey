package io.github.orange2652.partner.channel.saga.inbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.orange2652.partner.channel.persistence.saga.domain.SagaState;
import io.github.orange2652.partner.channel.persistence.saga.domain.SagaStateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

@ExtendWith(MockitoExtension.class)
class SagaStarterTest {

    @Mock
    private SagaStateRepository sagaStateRepository;

    @InjectMocks
    private SagaStarter sagaStarter;

    private SagaState newState;

    @BeforeEach
    void setUp() {
        newState = SagaState.start("A1_ORDER_RECEPTION", "12345", "STARTED", "{}");
    }

    @Test
    @DisplayName("정상 INSERT 시 true 반환")
    void startNewSaga() {
        given(sagaStateRepository.save(any(SagaState.class))).willReturn(newState);

        boolean started = sagaStarter.startIfAbsent(newState);

        assertThat(started).isTrue();
        verify(sagaStateRepository, times(1)).save(newState);
    }

    @Test
    @DisplayName("DataIntegrityViolationException (UNIQUE 위반) 시 false 반환 + 예외 전파 안 함")
    void duplicateSagaIsSkipped() {
        given(sagaStateRepository.save(any(SagaState.class)))
                .willThrow(new DataIntegrityViolationException("uk_saga_state_correlation"));

        boolean started = sagaStarter.startIfAbsent(newState);

        assertThat(started).isFalse();
    }
}
