package io.github.orange2652.partner.channel.saga.exception;

/**
 * saga command/reply 발행 실패. 직렬화 오류 / Kafka send 실패 / timeout 등을 포괄.
 *
 * <p>발행 실패 시 saga state 는 직전 단계에 stuck — 호출자가 catch 해 보상 또는 재시도
 * 결정을 한다.</p>
 */
public final class SagaPublishException extends SagaException {

    public SagaPublishException(String message) {
        super("SAGA_PUBLISH_FAILED", message);
    }

    public SagaPublishException(String message, Throwable cause) {
        super("SAGA_PUBLISH_FAILED", message, cause);
    }
}
