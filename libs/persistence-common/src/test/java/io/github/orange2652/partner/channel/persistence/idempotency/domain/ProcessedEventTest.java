package io.github.orange2652.partner.channel.persistence.idempotency.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.LocalDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ProcessedEventTest {

    private static final String CONSUMER_NAME = "channel-adapter";
    private static final String EVENT_ID = "0:42";

    @Nested
    @DisplayName("newRecord() 정적 팩토리")
    class NewRecordFactory {

        @Test
        @DisplayName("processedAt 자동 채움, 식별자 그대로")
        void freshRecord() {
            ProcessedEvent event = ProcessedEvent.newRecord(CONSUMER_NAME, EVENT_ID);

            assertThat(event.consumerName()).isEqualTo(CONSUMER_NAME);
            assertThat(event.eventId()).isEqualTo(EVENT_ID);
            assertThat(event.processedAt()).isNotNull();
        }

        @Test
        @DisplayName("두 번 호출하면 processedAt 은 같거나 단조 증가 — 두 인스턴스 식별자만 같음")
        void identityDependsOnKeyOnly() {
            ProcessedEvent a = ProcessedEvent.newRecord(CONSUMER_NAME, EVENT_ID);
            ProcessedEvent b = ProcessedEvent.newRecord(CONSUMER_NAME, EVENT_ID);

            assertThat(a.consumerName()).isEqualTo(b.consumerName());
            assertThat(a.eventId()).isEqualTo(b.eventId());
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
                    .isThrownBy(() -> new ProcessedEvent(null, EVENT_ID, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new ProcessedEvent(CONSUMER_NAME, null, now));
            assertThatNullPointerException()
                    .isThrownBy(() -> new ProcessedEvent(CONSUMER_NAME, EVENT_ID, null));
        }
    }
}
