package io.github.orange2652.partner.channel.client.toss;

/**
 * 토스 주문 상품 상태 상수 — 공식 문서의 {@code orderProductStatus} enum.
 *
 * <p>정상 흐름:</p>
 * <pre>
 *   BEFORE_PAYMENT → PAID → PREPARING_PRODUCT → DELIVERING → DELIVERED → CONFIRMED_ORDER
 * </pre>
 */
public final class TossOrderStatuses {

    public static final String BEFORE_PAYMENT = "BEFORE_PAYMENT";
    public static final String PAID = "PAID";
    public static final String PREPARING_PRODUCT = "PREPARING_PRODUCT";
    public static final String DELIVERING = "DELIVERING";
    public static final String DELIVERED = "DELIVERED";
    public static final String CONFIRMED_ORDER = "CONFIRMED_ORDER";
    public static final String CANCELED_PAYMENT = "CANCELED_PAYMENT";
    public static final String DELAY_SHIPPING = "DELAY_SHIPPING";

    private TossOrderStatuses() {
    }
}
