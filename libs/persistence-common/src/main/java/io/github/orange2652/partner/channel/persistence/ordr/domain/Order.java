package io.github.orange2652.partner.channel.persistence.ordr.domain;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * 내부 확정 주문 — A1 saga step 3 Pivot 통과 시 INSERT.
 *
 * <p>외부 식별자 ({@code channel, externalOrderProductId}) UNIQUE — 중복 확정 차단.
 * Pivot 통과 후 saga 보상 불가, 취소는 별도 saga (B2 — 다음 라운드).</p>
 *
 * @param id                     DB PK (신규는 null)
 * @param channel                채널 식별자
 * @param externalOrderId        외부 주문 ID
 * @param externalOrderProductId 외부 주문 상품 ID — UNIQUE
 * @param status                 내부 상태 (예: {@code "CREATED"} — Pivot 직후 초기 상태)
 * @param shipmentId             외부 물류 의뢰 식별자
 * @param raw                    외부 응답 원본 (JSON) — 재처리 / 감사용
 * @param createdAt              INSERT 시각
 * @param updatedAt              마지막 갱신 시각
 */
public record Order(
        Long id,
        String channel,
        String externalOrderId,
        String externalOrderProductId,
        String status,
        String shipmentId,
        String raw,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
    public Order {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(externalOrderId, "externalOrderId");
        Objects.requireNonNull(externalOrderProductId, "externalOrderProductId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(shipmentId, "shipmentId");
        Objects.requireNonNull(raw, "raw");
    }

    /**
     * Pivot 통과 직후의 새 주문 — {@code id=null}, {@code status="CREATED"}, 시각은 {@link LocalDateTime#now()}.
     */
    public static Order newRecord(String channel,
                                  String externalOrderId,
                                  String externalOrderProductId,
                                  String shipmentId,
                                  String raw) {
        LocalDateTime now = LocalDateTime.now();
        return new Order(null, channel, externalOrderId, externalOrderProductId,
                "CREATED", shipmentId, raw, now, now);
    }
}
