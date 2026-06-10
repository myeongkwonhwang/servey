package io.github.orange2652.partner.channel.event.saga;

/**
 * Kafka header {@code command-type} 값.
 *
 * <p>한 topic 안에 같은 step 의 request/reply 가 모두 흐를 수 있어, header 로 의미를 구분.</p>
 */
public final class SagaCommandTypes {

    public static final String UNCONFIRMED_ORDER_REQUEST = "UNCONFIRMED_ORDER_REQUEST";
    public static final String UNCONFIRMED_ORDER_REPLY_OK = "UNCONFIRMED_ORDER_REPLY_OK";
    public static final String UNCONFIRMED_ORDER_REPLY_FAIL = "UNCONFIRMED_ORDER_REPLY_FAIL";

    public static final String VALIDATE_REQUEST = "VALIDATE_REQUEST";
    public static final String VALIDATE_REPLY_OK = "VALIDATE_REPLY_OK";
    public static final String VALIDATE_REPLY_FAIL = "VALIDATE_REPLY_FAIL";

    public static final String CONFIRMED_ORDER_REQUEST = "CONFIRMED_ORDER_REQUEST";
    public static final String CONFIRMED_ORDER_REPLY_OK = "CONFIRMED_ORDER_REPLY_OK";
    public static final String CONFIRMED_ORDER_REPLY_FAIL = "CONFIRMED_ORDER_REPLY_FAIL";

    // 보상 흐름
    public static final String UNCONFIRMED_ORDER_COMPENSATE_REQUEST = "UNCONFIRMED_ORDER_COMPENSATE_REQUEST";
    public static final String UNCONFIRMED_ORDER_COMPENSATE_REPLY_OK = "UNCONFIRMED_ORDER_COMPENSATE_REPLY_OK";
    public static final String UNCONFIRMED_ORDER_COMPENSATE_REPLY_FAIL = "UNCONFIRMED_ORDER_COMPENSATE_REPLY_FAIL";

    private SagaCommandTypes() {
    }
}
