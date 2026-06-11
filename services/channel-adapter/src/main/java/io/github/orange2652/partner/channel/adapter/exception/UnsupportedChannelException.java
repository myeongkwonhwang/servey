package io.github.orange2652.partner.channel.adapter.exception;

/**
 * 지원하지 않는 채널 식별자. command/reply 에 새 채널이 들어왔거나 라우팅 미구현.
 */
public final class UnsupportedChannelException extends ChannelException {

    public UnsupportedChannelException(String message) {
        super("UNSUPPORTED_CHANNEL", message);
    }
}
