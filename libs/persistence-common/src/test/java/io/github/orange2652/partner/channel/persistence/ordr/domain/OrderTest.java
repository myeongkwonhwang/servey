package io.github.orange2652.partner.channel.persistence.ordr.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.github.orange2652.partner.channel.common.Channel;
import java.time.LocalDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class OrderTest {

    private static final String CHANNEL = Channel.TOSS.code();
    private static final String EXTERNAL_ORDER_ID = "100001";
    private static final String EXTERNAL_ORDER_PRODUCT_ID = "100001-1";
    private static final String SHIPMENT_ID = "MOCK-uuid";
    private static final String RAW = "{\"orderProductId\":\"100001-1\"}";

    @Nested
    @DisplayName("newRecord() 정적 팩토리")
    class NewRecordFactory {

        @Test
        @DisplayName("id=null, status=CREATED, createdAt==updatedAt, 시각 자동 채움")
        void unpersistedByDefault() {
            Order order = Order.newRecord(CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID, SHIPMENT_ID, RAW);

            assertThat(order.id()).isNull();
            assertThat(order.status()).isEqualTo("CREATED");
            assertThat(order.channel()).isEqualTo(CHANNEL);
            assertThat(order.externalOrderId()).isEqualTo(EXTERNAL_ORDER_ID);
            assertThat(order.externalOrderProductId()).isEqualTo(EXTERNAL_ORDER_PRODUCT_ID);
            assertThat(order.shipmentId()).isEqualTo(SHIPMENT_ID);
            assertThat(order.raw()).isEqualTo(RAW);
            assertThat(order.createdAt()).isNotNull();
            assertThat(order.updatedAt()).isEqualTo(order.createdAt());
        }
    }

    @Nested
    @DisplayName("null 방어")
    class NullDefense {

        @Test
        @DisplayName("필수 필드 누락 시 NPE")
        void requiredFields() {
            LocalDateTime now = LocalDateTime.now();
            assertThatNullPointerException()
                    .isThrownBy(() -> new Order(null, null, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            "CREATED", SHIPMENT_ID, RAW, now, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new Order(null, CHANNEL, null, EXTERNAL_ORDER_PRODUCT_ID,
                            "CREATED", SHIPMENT_ID, RAW, now, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new Order(null, CHANNEL, EXTERNAL_ORDER_ID, null,
                            "CREATED", SHIPMENT_ID, RAW, now, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new Order(null, CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            null, SHIPMENT_ID, RAW, now, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new Order(null, CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            "CREATED", null, RAW, now, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new Order(null, CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            "CREATED", SHIPMENT_ID, null, now, now));
        }
    }
}
