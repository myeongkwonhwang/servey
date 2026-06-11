package io.github.orange2652.partner.channel.adapter.exception;

/**
 * 외부 채널 페이로드 파싱/해석 실패. JSON 형식 오류 / 필수 필드 누락 등.
 */
public final class ChannelPayloadException extends ChannelException {

    public ChannelPayloadException(String message) {
        super("CHANNEL_PAYLOAD_INVALID", message);
    }

    public ChannelPayloadException(String message, Throwable cause) {
        super("CHANNEL_PAYLOAD_INVALID", message, cause);
    }
}
