package io.github.orange2652.partner.channel.persistence.staging.domain;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * A1 saga step 1 의 결과 — 외부 raw 주문을 비정규 schema 에 적재한 row.
 *
 * <p>{@code raw} 는 외부 응답을 JSON 으로 그대로 보존 (스펙 변경 시 재처리/감사 목적).
 * {@code typed} 필드는 자주 조회되는 식별자/상태/시간만 normalize.</p>
 *
 * <p>{@code status} (ACTIVE / CANCELED) — saga 보상 시 row 자체 DELETE 가 아닌
 * UPDATE 로 감사 추적 유지. step 2 FAIL 시 CANCELED 로 전이.</p>
 *
 * @param id                     DB PK (신규는 null)
 * @param channel                채널 식별자 (예: {@code "TOSS"})
 * @param externalOrderId        외부 주문 ID
 * @param externalOrderProductId 외부 주문 상품 ID — {@code (channel, externalOrderProductId)} UNIQUE
 * @param externalStatus         외부 상태 코드 (예: {@code "PAID"})
 * @param status                 내부 상태 — {@code "ACTIVE"} / {@code "CANCELED"}
 * @param orderedAt              외부 주문 시각
 * @param raw                    외부 응답 원본 (JSON)
 * @param receivedAt             내부 수신 시각
 */
public record StagingOrder(
        Long id,
        String channel,
        String externalOrderId,
        String externalOrderProductId,
        String externalStatus,
        String status,
        LocalDateTime orderedAt,
        String raw,
        LocalDateTime receivedAt
) {
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_CANCELED = "CANCELED";

    public StagingOrder {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(externalOrderId, "externalOrderId");
        Objects.requireNonNull(externalOrderProductId, "externalOrderProductId");
        Objects.requireNonNull(externalStatus, "externalStatus");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(orderedAt, "orderedAt");
        Objects.requireNonNull(raw, "raw");
    }

    /**
     * 신규 적재 row — {@code id=null}, {@code status=ACTIVE}, {@code receivedAt=now}.
     */
    public static StagingOrder newRecord(String channel,
                                         String externalOrderId,
                                         String externalOrderProductId,
                                         String externalStatus,
                                         LocalDateTime orderedAt,
                                         String raw) {
        return new StagingOrder(null, channel, externalOrderId, externalOrderProductId,
                externalStatus, STATUS_ACTIVE, orderedAt, raw, LocalDateTime.now());
    }

    /**
     * 보상 (취소) 전이 — {@code status=CANCELED} 새 인스턴스 반환 (불변).
     */
    public StagingOrder markCanceled() {
        return new StagingOrder(id, channel, externalOrderId, externalOrderProductId,
                externalStatus, STATUS_CANCELED, orderedAt, raw, receivedAt);
    }

    public boolean isCanceled() {
        return STATUS_CANCELED.equals(status);
    }
}
