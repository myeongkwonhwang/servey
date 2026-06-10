package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 보상 reply payload.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record UnconfirmedOrderCompensateReply(
        Result result,
        String errorCode,
        String errorMessage
) {
    public enum Result {
        OK, FAILED
    }

    public static UnconfirmedOrderCompensateReply ok() {
        return new UnconfirmedOrderCompensateReply(Result.OK, null, null);
    }

    public static UnconfirmedOrderCompensateReply failed(String errorCode, String errorMessage) {
        return new UnconfirmedOrderCompensateReply(Result.FAILED, errorCode, errorMessage);
    }
}
