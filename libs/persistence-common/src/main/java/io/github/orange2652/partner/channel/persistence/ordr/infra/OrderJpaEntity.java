package io.github.orange2652.partner.channel.persistence.ordr.infra;

import io.github.orange2652.partner.channel.persistence.ordr.domain.Order;
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

/**
 * JPA Entity — {@code core_schema.orders} 매핑.
 *
 * <p>package-private — 외부에서는 도메인 record + Port 로만 접근.</p>
 */
@Entity
@Table(name = "orders", schema = "core_schema")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
class OrderJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "channel", nullable = false, length = 32)
    private String channel;

    @Column(name = "external_order_id", nullable = false, length = 64)
    private String externalOrderId;

    @Column(name = "external_order_product_id", nullable = false, length = 64)
    private String externalOrderProductId;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "shipment_id", nullable = false, length = 64)
    private String shipmentId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw", nullable = false, columnDefinition = "jsonb")
    private String raw;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    static OrderJpaEntity from(Order order) {
        return OrderJpaEntity.builder()
                .id(order.id())
                .channel(order.channel())
                .externalOrderId(order.externalOrderId())
                .externalOrderProductId(order.externalOrderProductId())
                .status(order.status())
                .shipmentId(order.shipmentId())
                .raw(order.raw())
                .createdAt(order.createdAt())
                .updatedAt(order.updatedAt())
                .build();
    }

    Order toDomain() {
        return new Order(id, channel, externalOrderId, externalOrderProductId,
                status, shipmentId, raw, createdAt, updatedAt);
    }
}
