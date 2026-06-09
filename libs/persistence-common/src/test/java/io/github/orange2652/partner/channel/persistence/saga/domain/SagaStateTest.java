package io.github.orange2652.partner.channel.persistence.saga.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SagaStateTest {

    private static final String SAGA_TYPE = "A1_ORDER_RECEPTION";
    private static final String CORRELATION = "12345";
    private static final String INITIAL_STEP = "STARTED";
    private static final String PAYLOAD = "{\"orderProductId\":12345}";

    @Nested
    @DisplayName("start() 정적 팩토리")
    class StartFactory {

        @Test
        @DisplayName("기본 상태는 RUNNING, currentStep 은 initialStep, sagaId 자동 생성")
        void newSagaIsRunning() {
            SagaState state = SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, PAYLOAD);

            assertThat(state.status()).isEqualTo(SagaStatus.RUNNING);
            assertThat(state.currentStep()).isEqualTo(INITIAL_STEP);
            assertThat(state.sagaType()).isEqualTo(SAGA_TYPE);
            assertThat(state.correlationKey()).isEqualTo(CORRELATION);
            assertThat(state.payload()).isEqualTo(PAYLOAD);
            assertThat(state.sagaId()).isNotNull();
            assertThat(state.startedAt()).isEqualTo(state.updatedAt());
        }

        @Test
        @DisplayName("동일 입력 두 번 호출 시 sagaId 는 매번 새로 생성")
        void sagaIdIsUnique() {
            SagaState a = SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, PAYLOAD);
            SagaState b = SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, PAYLOAD);

            assertThat(a.sagaId()).isNotEqualTo(b.sagaId());
        }
    }

    @Nested
    @DisplayName("상태 전이")
    class Transitions {

        @Test
        @DisplayName("advance() 는 currentStep 만 갱신, sagaId/startedAt 유지")
        void advancePreservesIdentity() {
            SagaState state = SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, PAYLOAD);

            SagaState advanced = state.advance("STAGING_INSERTED");

            assertThat(advanced.sagaId()).isEqualTo(state.sagaId());
            assertThat(advanced.startedAt()).isEqualTo(state.startedAt());
            assertThat(advanced.currentStep()).isEqualTo("STAGING_INSERTED");
            assertThat(advanced.status()).isEqualTo(SagaStatus.RUNNING);
        }

        @Test
        @DisplayName("compensate() 는 status 만 COMPENSATING 으로")
        void compensateChangesStatus() {
            SagaState state = SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, PAYLOAD);

            assertThat(state.compensate().status()).isEqualTo(SagaStatus.COMPENSATING);
        }

        @Test
        @DisplayName("complete() / abort() 는 종결 상태로 전이")
        void terminalTransitions() {
            SagaState state = SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, PAYLOAD);

            assertThat(state.complete().status()).isEqualTo(SagaStatus.COMPLETED);
            assertThat(state.abort().status()).isEqualTo(SagaStatus.ABORTED);
        }
    }

    @Nested
    @DisplayName("null 방어")
    class NullDefense {

        @Test
        @DisplayName("필수 필드 누락 시 NPE")
        void requiredFields() {
            assertThatNullPointerException()
                    .isThrownBy(() -> SagaState.start(null, CORRELATION, INITIAL_STEP, PAYLOAD));
            assertThatNullPointerException()
                    .isThrownBy(() -> SagaState.start(SAGA_TYPE, null, INITIAL_STEP, PAYLOAD));
            assertThatNullPointerException()
                    .isThrownBy(() -> SagaState.start(SAGA_TYPE, CORRELATION, null, PAYLOAD));
            assertThatNullPointerException()
                    .isThrownBy(() -> SagaState.start(SAGA_TYPE, CORRELATION, INITIAL_STEP, null));
        }
    }
}
