package io.github.orange2652.partner.channel.event.saga;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A1 step 2 — validate reply payload.
 *
 * <p>service-core 가 발행 → saga-orchestrator 가 수신해 saga state advance.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ValidateReply(
        Result result,
        String errorCode,
        String errorMessage
) {
    public enum Result {
        OK, FAILED
    }

    public static ValidateReply ok() {
        return new ValidateReply(Result.OK, null, null);
    }

    public static ValidateReply failed(String errorCode, String errorMessage) {
        return new ValidateReply(Result.FAILED, errorCode, errorMessage);
    }
}
