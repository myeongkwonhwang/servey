package io.github.orange2652.partner.channel.event.saga;

/**
 * Kafka header {@code command-type} 값.
 *
 * <p>한 topic 안에 같은 step 의 request/reply 가 모두 흐를 수 있어, header 로 의미를 구분.</p>
 *
 * <p><b>명명 정책 (의도적 어휘 분리)</b></p>
 * <ul>
 *   <li><b>Command</b> — 도메인 record (예: {@code UnconfirmedOrderCommand}). DDD 명령 객체.</li>
 *   <li><b>*_REQUEST / *_REPLY_OK / *_REPLY_FAIL</b> — Kafka 통신 메시지 어휘. 한 step 의 요청/응답 쌍을 표현.</li>
 * </ul>
 * <p>두 어휘는 다른 레이어 (도메인 모델 vs 메시지 라우팅) 의 표현. 명명 충돌이 아닌 의도적
 * 분리이며, 한쪽으로 통일하지 않는다.</p>
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
