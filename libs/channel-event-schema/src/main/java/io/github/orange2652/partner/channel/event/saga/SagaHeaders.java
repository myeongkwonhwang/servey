package io.github.orange2652.partner.channel.event.saga;

/**
 * Saga 메시지에 실리는 Kafka header key.
 *
 * <p>본문 (payload) 은 step 별 command/reply DTO. header 는 saga 메타데이터 (sagaId, step, type).
 * payload parse 없이 header 만으로 라우팅·필터링이 가능하도록.</p>
 */
public final class SagaHeaders {

    public static final String SAGA_ID = "saga-id";
    public static final String SAGA_TYPE = "saga-type";
    public static final String SAGA_STEP = "saga-step";
    public static final String COMMAND_TYPE = "command-type";

    private SagaHeaders() {
    }
}
