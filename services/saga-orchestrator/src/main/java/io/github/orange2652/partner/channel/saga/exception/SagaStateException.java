package io.github.orange2652.partner.channel.saga.exception;

/**
 * saga state 조회/전이 불가. sagaId 로 찾았으나 row 없음 / 예상하지 못한 상태 등.
 */
public final class SagaStateException extends SagaException {

    public SagaStateException(String message) {
        super("SAGA_STATE_INVALID", message);
    }

    public SagaStateException(String message, Throwable cause) {
        super("SAGA_STATE_INVALID", message, cause);
    }
}
