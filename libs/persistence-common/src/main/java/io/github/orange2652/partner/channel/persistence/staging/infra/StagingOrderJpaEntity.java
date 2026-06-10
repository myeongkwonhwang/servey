package io.github.orange2652.partner.channel.persistence.staging.infra;

import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrder;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "staging_order", schema = "channel_schema")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
class StagingOrderJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "channel", nullable = false, length = 32)
    private String channel;

    @Column(name = "external_order_id", nullable = false, length = 64)
    private String externalOrderId;

    @Column(name = "external_order_product_id", nullable = false, length = 64)
    private String externalOrderProductId;

    @Column(name = "external_status", nullable = false, length = 64)
    private String externalStatus;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "ordered_at", nullable = false)
    private LocalDateTime orderedAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw", nullable = false, columnDefinition = "jsonb")
    private String raw;

    @Column(name = "received_at", nullable = false)
    private LocalDateTime receivedAt;

    static StagingOrderJpaEntity from(StagingOrder order) {
        return StagingOrderJpaEntity.builder()
                .id(order.id())
                .channel(order.channel())
                .externalOrderId(order.externalOrderId())
                .externalOrderProductId(order.externalOrderProductId())
                .externalStatus(order.externalStatus())
                .status(order.status())
                .orderedAt(order.orderedAt())
                .raw(order.raw())
                .receivedAt(order.receivedAt())
                .build();
    }

    StagingOrder toDomain() {
        return new StagingOrder(id, channel, externalOrderId, externalOrderProductId,
                externalStatus, status, orderedAt, raw, receivedAt);
    }
}
