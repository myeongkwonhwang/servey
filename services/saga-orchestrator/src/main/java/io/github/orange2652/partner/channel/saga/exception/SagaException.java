package io.github.orange2652.partner.channel.saga.exception;

import io.github.orange2652.partner.channel.common.exception.PartnerChannelException;

/**
 * saga-orchestrator 도메인 예외 베이스. saga lifecycle 자체에서 발생.
 */
public abstract class SagaException extends PartnerChannelException {

    protected SagaException(String code, String message) {
        super(code, message);
    }

    protected SagaException(String code, String message, Throwable cause) {
        super(code, message, cause);
    }
}
