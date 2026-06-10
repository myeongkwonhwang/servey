package io.github.orange2652.partner.channel.client.toss;

/**
 * 토스 판매자 취소 응답.
 *
 * @param success      성공 여부
 * @param errorCode    실패 시 코드 ({@code INVALID_REQUEST} / {@code COMMON_ERROR})
 * @param errorReason  실패 시 사유
 */
public record CancelOrderResult(
        boolean success,
        String errorCode,
        String errorReason
) {
    public static CancelOrderResult ok() {
        return new CancelOrderResult(true, null, null);
    }

    public static CancelOrderResult failed(String errorCode, String errorReason) {
        return new CancelOrderResult(false, errorCode, errorReason);
    }
}
