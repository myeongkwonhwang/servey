package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A1 step 1 — unconfirmedOrder reply payload.
 *
 * <p>channel-adapter 가 발행 → saga-orchestrator 가 수신해 saga state advance.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record UnconfirmedOrderReply(
        Result result,
        Long stagingId,
        String errorCode,
        String errorMessage
) {
    public enum Result {
        OK, FAILED
    }

    public static UnconfirmedOrderReply ok(Long stagingId) {
        return new UnconfirmedOrderReply(Result.OK, stagingId, null, null);
    }

    public static UnconfirmedOrderReply failed(String errorCode, String errorMessage) {
        return new UnconfirmedOrderReply(Result.FAILED, null, errorCode, errorMessage);
    }
}
