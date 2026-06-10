package io.github.orange2652.partner.channel.persistence.outbox.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.LocalDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class OutboxEventTest {

    private static final String AGG_TYPE = "ORDER";
    private static final String AGG_ID = "12345";
    private static final String EVENT_TYPE = "ChannelOrderReceived";
    private static final String PAYLOAD = "{\"orderProductId\":12345}";
    private static final String HEADERS = "{\"channel\":\"TOSS\"}";

    @Nested
    @DisplayName("newEvent() 정적 팩토리")
    class NewEventFactory {

        @Test
        @DisplayName("기본 상태 — id/publishedAt null, createdAt 채움, isPublished=false")
        void unpublishedByDefault() {
            OutboxEvent event = OutboxEvent.newEvent(AGG_TYPE, AGG_ID, EVENT_TYPE, PAYLOAD, HEADERS);

            assertThat(event.id()).isNull();
            assertThat(event.publishedAt()).isNull();
            assertThat(event.createdAt()).isNotNull();
            assertThat(event.isPublished()).isFalse();
            assertThat(event.aggregateType()).isEqualTo(AGG_TYPE);
            assertThat(event.aggregateId()).isEqualTo(AGG_ID);
            assertThat(event.eventType()).isEqualTo(EVENT_TYPE);
            assertThat(event.payload()).isEqualTo(PAYLOAD);
            assertThat(event.headers()).isEqualTo(HEADERS);
        }

        @Test
        @DisplayName("headers 는 nullable — null 허용")
        void headersIsOptional() {
            OutboxEvent event = OutboxEvent.newEvent(AGG_TYPE, AGG_ID, EVENT_TYPE, PAYLOAD, null);

            assertThat(event.headers()).isNull();
        }
    }

    @Nested
    @DisplayName("markPublished()")
    class MarkPublished {

        @Test
        @DisplayName("publishedAt 만 갱신, 나머지 필드는 그대로")
        void onlyPublishedAtChanges() {
            OutboxEvent event = OutboxEvent.newEvent(AGG_TYPE, AGG_ID, EVENT_TYPE, PAYLOAD, HEADERS);
            LocalDateTime publishedAt = LocalDateTime.parse("2026-06-09T00:00:00");

            OutboxEvent published = event.markPublished(publishedAt);

            assertThat(published.publishedAt()).isEqualTo(publishedAt);
            assertThat(published.isPublished()).isTrue();
            assertThat(published.id()).isEqualTo(event.id());
            assertThat(published.aggregateType()).isEqualTo(event.aggregateType());
            assertThat(published.aggregateId()).isEqualTo(event.aggregateId());
            assertThat(published.eventType()).isEqualTo(event.eventType());
            assertThat(published.payload()).isEqualTo(event.payload());
            assertThat(published.headers()).isEqualTo(event.headers());
            assertThat(published.createdAt()).isEqualTo(event.createdAt());
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
                    .isThrownBy(() -> new OutboxEvent(null, null, AGG_ID, EVENT_TYPE, PAYLOAD, HEADERS, now, null));
            assertThatNullPointerException()
                    .isThrownBy(() -> new OutboxEvent(null, AGG_TYPE, null, EVENT_TYPE, PAYLOAD, HEADERS, now, null));
            assertThatNullPointerException()
                    .isThrownBy(() -> new OutboxEvent(null, AGG_TYPE, AGG_ID, null, PAYLOAD, HEADERS, now, null));
            assertThatNullPointerException()
                    .isThrownBy(() -> new OutboxEvent(null, AGG_TYPE, AGG_ID, EVENT_TYPE, null, HEADERS, now, null));
            assertThatNullPointerException()
                    .isThrownBy(() -> new OutboxEvent(null, AGG_TYPE, AGG_ID, EVENT_TYPE, PAYLOAD, HEADERS, null, null));
        }
    }
}
