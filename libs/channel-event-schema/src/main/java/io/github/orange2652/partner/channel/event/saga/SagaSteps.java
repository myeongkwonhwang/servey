package io.github.orange2652.partner.channel.event.saga;

/**
 * A1 saga 의 currentStep 라벨.
 *
 * <p>흐름:</p>
 * <pre>
 *   STARTED → UNCONFIRMED_ORDER_SENT → UNCONFIRMED_ORDER_INSERTED
 *           → VALIDATE_SENT → VALIDATED (또는 VALIDATE_FAILED)
 *           → CONFIRMED_ORDER_SENT → CONFIRMED_ORDER_PERSISTED (Pivot ★, 또는 CONFIRMED_ORDER_FAILED)
 * </pre>
 *
 * <p>{@code _SENT} 는 command 발행 직후, {@code _INSERTED} (또는 step 별 종결 라벨) 는 reply 수신 직후.
 * 두 상태로 발행 실패/유실 진단 가능.</p>
 */
public final class SagaSteps {

    public static final String STARTED = "STARTED";
    public static final String UNCONFIRMED_ORDER_SENT = "UNCONFIRMED_ORDER_SENT";
    public static final String UNCONFIRMED_ORDER_INSERTED = "UNCONFIRMED_ORDER_INSERTED";
    public static final String VALIDATE_SENT = "VALIDATE_SENT";
    public static final String VALIDATED = "VALIDATED";
    public static final String VALIDATE_FAILED = "VALIDATE_FAILED";
    public static final String CONFIRMED_ORDER_SENT = "CONFIRMED_ORDER_SENT";
    public static final String CONFIRMED_ORDER_PERSISTED = "CONFIRMED_ORDER_PERSISTED";
    public static final String CONFIRMED_ORDER_FAILED = "CONFIRMED_ORDER_FAILED";

    // 보상 흐름 (step 2 FAIL → step 1 보상)
    public static final String UNCONFIRMED_ORDER_COMPENSATE_SENT = "UNCONFIRMED_ORDER_COMPENSATE_SENT";
    public static final String UNCONFIRMED_ORDER_COMPENSATED = "UNCONFIRMED_ORDER_COMPENSATED";
    public static final String UNCONFIRMED_ORDER_COMPENSATE_FAILED = "UNCONFIRMED_ORDER_COMPENSATE_FAILED";

    private SagaSteps() {
    }
}
