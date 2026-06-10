package io.github.orange2652.partner.channel.persistence.staging.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.LocalDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StagingOrderTest {

    private static final String CHANNEL = "TOSS";
    private static final String EXTERNAL_ORDER_ID = "100001";
    private static final String EXTERNAL_ORDER_PRODUCT_ID = "100001-1";
    private static final String EXTERNAL_STATUS = "PAID";
    private static final LocalDateTime ORDERED_AT = LocalDateTime.parse("2026-06-09T00:00:00");
    private static final String RAW = "{\"orderProductId\":\"100001-1\"}";

    @Nested
    @DisplayName("newRecord() 정적 팩토리")
    class NewRecordFactory {

        @Test
        @DisplayName("id=null, status=ACTIVE, receivedAt 자동 채움, 다른 필드 그대로")
        void unpersistedByDefault() {
            StagingOrder order = StagingOrder.newRecord(
                    CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                    EXTERNAL_STATUS, ORDERED_AT, RAW);

            assertThat(order.id()).isNull();
            assertThat(order.status()).isEqualTo(StagingOrder.STATUS_ACTIVE);
            assertThat(order.isCanceled()).isFalse();
            assertThat(order.receivedAt()).isNotNull();
            assertThat(order.channel()).isEqualTo(CHANNEL);
            assertThat(order.externalOrderId()).isEqualTo(EXTERNAL_ORDER_ID);
            assertThat(order.externalOrderProductId()).isEqualTo(EXTERNAL_ORDER_PRODUCT_ID);
            assertThat(order.externalStatus()).isEqualTo(EXTERNAL_STATUS);
            assertThat(order.orderedAt()).isEqualTo(ORDERED_AT);
            assertThat(order.raw()).isEqualTo(RAW);
        }
    }

    @Nested
    @DisplayName("markCanceled() — 보상 전이")
    class MarkCanceled {

        @Test
        @DisplayName("status 만 CANCELED, 나머지 필드는 그대로 (id 유지)")
        void onlyStatusChanges() {
            StagingOrder order = StagingOrder.newRecord(
                    CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                    EXTERNAL_STATUS, ORDERED_AT, RAW);

            StagingOrder canceled = order.markCanceled();

            assertThat(canceled.status()).isEqualTo(StagingOrder.STATUS_CANCELED);
            assertThat(canceled.isCanceled()).isTrue();
            assertThat(canceled.channel()).isEqualTo(order.channel());
            assertThat(canceled.externalOrderProductId()).isEqualTo(order.externalOrderProductId());
            assertThat(canceled.externalStatus()).isEqualTo(order.externalStatus());
            assertThat(canceled.raw()).isEqualTo(order.raw());
            assertThat(canceled.receivedAt()).isEqualTo(order.receivedAt());
        }
    }

    @Nested
    @DisplayName("null 방어")
    class NullDefense {

        @Test
        @DisplayName("필수 필드 누락 시 NPE")
        void requiredFields() {
            LocalDateTime now = LocalDateTime.now();
            String active = StagingOrder.STATUS_ACTIVE;
            assertThatNullPointerException()
                    .isThrownBy(() -> new StagingOrder(null, null, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            EXTERNAL_STATUS, active, ORDERED_AT, RAW, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new StagingOrder(null, CHANNEL, null, EXTERNAL_ORDER_PRODUCT_ID,
                            EXTERNAL_STATUS, active, ORDERED_AT, RAW, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new StagingOrder(null, CHANNEL, EXTERNAL_ORDER_ID, null,
                            EXTERNAL_STATUS, active, ORDERED_AT, RAW, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new StagingOrder(null, CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            null, active, ORDERED_AT, RAW, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new StagingOrder(null, CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            EXTERNAL_STATUS, null, ORDERED_AT, RAW, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new StagingOrder(null, CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            EXTERNAL_STATUS, active, null, RAW, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new StagingOrder(null, CHANNEL, EXTERNAL_ORDER_ID, EXTERNAL_ORDER_PRODUCT_ID,
                            EXTERNAL_STATUS, active, ORDERED_AT, null, now));
        }
    }
}
