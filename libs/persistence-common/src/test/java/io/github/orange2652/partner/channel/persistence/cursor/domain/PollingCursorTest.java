package io.github.orange2652.partner.channel.persistence.cursor.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.github.orange2652.partner.channel.common.Channel;
import java.time.LocalDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PollingCursorTest {

    private static final String CHANNEL = Channel.TOSS.code();
    private static final String RESOURCE = "ORDER";
    private static final LocalDateTime WINDOW_START = LocalDateTime.parse("2026-06-09T00:00:00");
    private static final LocalDateTime WINDOW_END = LocalDateTime.parse("2026-06-09T01:00:00");

    @Nested
    @DisplayName("initial() 정적 팩토리")
    class InitialFactory {

        @Test
        @DisplayName("nextCursor=null, updatedAt 채움, windowCompleted=true")
        void freshCursor() {
            PollingCursor cursor = PollingCursor.initial(CHANNEL, RESOURCE, WINDOW_START, WINDOW_END);

            assertThat(cursor.channel()).isEqualTo(CHANNEL);
            assertThat(cursor.resource()).isEqualTo(RESOURCE);
            assertThat(cursor.windowStart()).isEqualTo(WINDOW_START);
            assertThat(cursor.windowEnd()).isEqualTo(WINDOW_END);
            assertThat(cursor.nextCursor()).isNull();
            assertThat(cursor.updatedAt()).isNotNull();
            assertThat(cursor.windowCompleted()).isTrue();
        }
    }

    @Nested
    @DisplayName("advance() — 페이지네이션 진행")
    class Advance {

        @Test
        @DisplayName("nextCursor 갱신, 윈도우/식별자 유지, windowCompleted=false")
        void advanceSetsNextCursor() {
            PollingCursor cursor = PollingCursor.initial(CHANNEL, RESOURCE, WINDOW_START, WINDOW_END);

            PollingCursor advanced = cursor.advance("page-2-token");

            assertThat(advanced.nextCursor()).isEqualTo("page-2-token");
            assertThat(advanced.windowCompleted()).isFalse();
            assertThat(advanced.channel()).isEqualTo(cursor.channel());
            assertThat(advanced.resource()).isEqualTo(cursor.resource());
            assertThat(advanced.windowStart()).isEqualTo(cursor.windowStart());
            assertThat(advanced.windowEnd()).isEqualTo(cursor.windowEnd());
        }
    }

    @Nested
    @DisplayName("rollWindow() — 다음 윈도우로 전환")
    class RollWindow {

        @Test
        @DisplayName("윈도우 교체 + nextCursor null 로 리셋")
        void rollSetsNewWindow() {
            PollingCursor cursor = PollingCursor.initial(CHANNEL, RESOURCE, WINDOW_START, WINDOW_END)
                    .advance("mid-token");
            LocalDateTime nextStart = WINDOW_END;
            LocalDateTime nextEnd = WINDOW_END.plusSeconds(3600);

            PollingCursor rolled = cursor.rollWindow(nextStart, nextEnd);

            assertThat(rolled.windowStart()).isEqualTo(nextStart);
            assertThat(rolled.windowEnd()).isEqualTo(nextEnd);
            assertThat(rolled.nextCursor()).isNull();
            assertThat(rolled.windowCompleted()).isTrue();
            assertThat(rolled.channel()).isEqualTo(cursor.channel());
            assertThat(rolled.resource()).isEqualTo(cursor.resource());
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
                    .isThrownBy(() -> new PollingCursor(null, RESOURCE, WINDOW_START, WINDOW_END, null, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new PollingCursor(CHANNEL, null, WINDOW_START, WINDOW_END, null, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new PollingCursor(CHANNEL, RESOURCE, null, WINDOW_END, null, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new PollingCursor(CHANNEL, RESOURCE, WINDOW_START, null, null, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new PollingCursor(CHANNEL, RESOURCE, WINDOW_START, WINDOW_END, null, null));
        }
    }
}
