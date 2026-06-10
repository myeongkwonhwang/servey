package io.github.orange2652.partner.channel.event.saga;

/**
 * Saga 종류 식별자.
 *
 * <p>{@code (sagaType, correlationKey)} 는 saga 인스턴스의 자연 키. 동일 외부 주문에 대해
 * 같은 saga 가 두 번 시작되는 것을 DB UNIQUE 가 차단.</p>
 */
public final class SagaTypes {

    public static final String ORDER_RECEPTION = "ORDER_RECEPTION";

    private SagaTypes() {
    }
}
