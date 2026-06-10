package io.github.orange2652.partner.channel.batch.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxEvent;
import io.github.orange2652.partner.channel.persistence.outbox.domain.OutboxRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    private static final String TOPIC = "channel.order.received";
    private static final String AGG_TYPE = "ORDER";
    private static final String EVENT_TYPE = "ChannelOrderReceived";

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private OutboxTopicRouter topicRouter;

    @InjectMocks
    private OutboxPublisher publisher;

    @Test
    @DisplayName("미발행 0 건이면 어떠한 send 도 markPublished 도 호출하지 않음")
    void noEventsNoOp() {
        given(outboxRepository.findUnpublished(OutboxPublisher.BATCH_SIZE)).willReturn(List.of());

        publisher.publish();

        verify(kafkaTemplate, never()).send(any(), any(), any());
        verify(outboxRepository, never()).markPublished(anyLong(), any());
    }

    @Test
    @DisplayName("3 건 모두 정상 발행 — 3 번 send + 3 번 markPublished")
    void publishAllSuccess() {
        OutboxEvent e1 = stored(1L, "k1", "{\"a\":1}");
        OutboxEvent e2 = stored(2L, "k2", "{\"a\":2}");
        OutboxEvent e3 = stored(3L, "k3", "{\"a\":3}");
        given(outboxRepository.findUnpublished(OutboxPublisher.BATCH_SIZE)).willReturn(List.of(e1, e2, e3));
        given(topicRouter.resolve(AGG_TYPE, EVENT_TYPE)).willReturn(TOPIC);
        given(kafkaTemplate.send(eq(TOPIC), any(), any())).willReturn(success());

        publisher.publish();

        verify(kafkaTemplate, times(3)).send(eq(TOPIC), any(String.class), any(String.class));

        ArgumentCaptor<Long> idCaptor = ArgumentCaptor.forClass(Long.class);
        verify(outboxRepository, times(3)).markPublished(idCaptor.capture(), any(LocalDateTime.class));
        assertThat(idCaptor.getAllValues()).containsExactly(1L, 2L, 3L);
    }

    @Test
    @DisplayName("2 번째 send 실패 시 break — 첫 1 건만 markPublished, 3 번째는 시도조차 안 함")
    void breakOnFailure() {
        OutboxEvent e1 = stored(1L, "k1", "{\"a\":1}");
        OutboxEvent e2 = stored(2L, "k2", "{\"a\":2}");
        OutboxEvent e3 = stored(3L, "k3", "{\"a\":3}");
        given(outboxRepository.findUnpublished(OutboxPublisher.BATCH_SIZE)).willReturn(List.of(e1, e2, e3));
        given(topicRouter.resolve(AGG_TYPE, EVENT_TYPE)).willReturn(TOPIC);
        given(kafkaTemplate.send(eq(TOPIC), eq("k1"), any())).willReturn(success());
        given(kafkaTemplate.send(eq(TOPIC), eq("k2"), any())).willReturn(failed());

        publisher.publish();

        verify(kafkaTemplate, times(2)).send(eq(TOPIC), any(String.class), any(String.class));
        verify(outboxRepository, times(1)).markPublished(eq(1L), any(LocalDateTime.class));
        verify(outboxRepository, never()).markPublished(eq(2L), any(LocalDateTime.class));
        verify(outboxRepository, never()).markPublished(eq(3L), any(LocalDateTime.class));
    }

    @Test
    @DisplayName("Kafka 발행 시 key=aggregateId, value=payload 로 보냄")
    void sendCarriesAggregateIdAndPayload() {
        OutboxEvent e1 = stored(7L, "order-7", "{\"orderProductId\":7}");
        given(outboxRepository.findUnpublished(OutboxPublisher.BATCH_SIZE)).willReturn(List.of(e1));
        given(topicRouter.resolve(AGG_TYPE, EVENT_TYPE)).willReturn(TOPIC);
        given(kafkaTemplate.send(eq(TOPIC), any(), any())).willReturn(success());

        publisher.publish();

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), valueCaptor.capture());
        assertThat(keyCaptor.getValue()).isEqualTo("order-7");
        assertThat(valueCaptor.getValue()).isEqualTo("{\"orderProductId\":7}");
        verify(outboxRepository).markPublished(eq(7L), any(LocalDateTime.class));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<SendResult<String, String>> success() {
        SendResult<String, String> mockResult = mock(SendResult.class);
        return CompletableFuture.completedFuture(mockResult);
    }

    private static CompletableFuture<SendResult<String, String>> failed() {
        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("kafka send failed"));
        return future;
    }

    private static OutboxEvent stored(long id, String aggregateId, String payload) {
        return new OutboxEvent(id, AGG_TYPE, aggregateId, EVENT_TYPE, payload, null,
                LocalDateTime.parse("2026-06-09T00:00:00"), null);
    }
}
