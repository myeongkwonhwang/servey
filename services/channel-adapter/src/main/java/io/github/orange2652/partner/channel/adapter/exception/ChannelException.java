package io.github.orange2652.partner.channel.adapter.exception;

import io.github.orange2652.partner.channel.common.exception.PartnerChannelException;

/**
 * channel-adapter 도메인 예외 베이스. 외부 채널 (TOSS / NAVER / ...) 연동 과정에서 발생.
 */
public abstract class ChannelException extends PartnerChannelException {

    protected ChannelException(String code, String message) {
        super(code, message);
    }

    protected ChannelException(String code, String message, Throwable cause) {
        super(code, message, cause);
    }
}
