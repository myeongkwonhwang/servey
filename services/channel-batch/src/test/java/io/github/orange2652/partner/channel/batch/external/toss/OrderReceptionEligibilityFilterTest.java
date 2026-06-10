package io.github.orange2652.partner.channel.batch.external.toss;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class OrderReceptionEligibilityFilterTest {

    private final OrderReceptionEligibilityFilter filter = new OrderReceptionEligibilityFilter();
    private static final LocalDateTime NOW = LocalDateTime.parse("2026-06-10T12:00:00");

    @Nested
    @DisplayName("RECEPTION_DELAY = 30분 기준")
    class ReceptionDelay {

        @Test
        @DisplayName("30분 이상 경과 → eligible")
        void thirtyMinAgoIsEligible() {
            LocalDateTime orderedAt = NOW.minusMinutes(31);

            assertThat(filter.isEligible(orderedAt, NOW)).isTrue();
        }

        @Test
        @DisplayName("정확히 30분 경과 → eligible (boundary)")
        void exactlyThirtyMinIsEligible() {
            LocalDateTime orderedAt = NOW.minusMinutes(30);

            assertThat(filter.isEligible(orderedAt, NOW)).isTrue();
        }

        @Test
        @DisplayName("29분 경과 → 미충족")
        void twentyNineMinIsNotEligible() {
            LocalDateTime orderedAt = NOW.minusMinutes(29);

            assertThat(filter.isEligible(orderedAt, NOW)).isFalse();
        }

        @Test
        @DisplayName("미래 시각 (외부 응답 오류) → 미충족")
        void futureIsNotEligible() {
            LocalDateTime orderedAt = NOW.plusMinutes(5);

            assertThat(filter.isEligible(orderedAt, NOW)).isFalse();
        }
    }
}
