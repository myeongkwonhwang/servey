package io.github.orange2652.partner.channel.saga.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.event.saga.SagaCommandTypes;
import io.github.orange2652.partner.channel.event.saga.SagaHeaders;
import io.github.orange2652.partner.channel.event.saga.SagaSteps;
import io.github.orange2652.partner.channel.event.saga.SagaTopics;
import io.github.orange2652.partner.channel.event.saga.SagaTypes;
import io.github.orange2652.partner.channel.saga.exception.SagaPublishException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
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
class SagaCommandPublisherTest {

    private static final UUID SAGA_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final String SERIALIZED = "{\"channel\":\"TOSS\"}";

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private SagaCommandPublisher publisher;

    @Test
    @DisplayName("정상 발행 — ProducerRecord 에 4개 header 정상 설정")
    void publishesWithHeaders() throws Exception {
        Object command = new Object();
        given(objectMapper.writeValueAsString(command)).willReturn(SERIALIZED);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(null);
        given(kafkaTemplate.send(any(ProducerRecord.class))).willReturn(future);

        publisher.publish(SAGA_ID, SagaSteps.UNCONFIRMED_ORDER_SENT,
                SagaCommandTypes.UNCONFIRMED_ORDER_REQUEST, command);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();

        assertThat(record.topic()).isEqualTo(SagaTopics.ORDER_CMD);
        assertThat(record.key()).isEqualTo(SAGA_ID.toString());
        assertThat(record.value()).isEqualTo(SERIALIZED);
        assertThat(headerValue(record, SagaHeaders.SAGA_ID)).isEqualTo(SAGA_ID.toString());
        assertThat(headerValue(record, SagaHeaders.SAGA_TYPE)).isEqualTo(SagaTypes.ORDER_RECEPTION);
        assertThat(headerValue(record, SagaHeaders.SAGA_STEP)).isEqualTo(SagaSteps.UNCONFIRMED_ORDER_SENT);
        assertThat(headerValue(record, SagaHeaders.COMMAND_TYPE)).isEqualTo(SagaCommandTypes.UNCONFIRMED_ORDER_REQUEST);
    }

    @Test
    @DisplayName("직렬화 실패 — SagaPublishException throw, send 호출 안 됨")
    void serializeFailureThrows() throws Exception {
        Object command = new Object();
        given(objectMapper.writeValueAsString(command))
                .willThrow(new JsonProcessingException("boom") {});

        assertThatThrownBy(() -> publisher.publish(SAGA_ID, SagaSteps.VALIDATE_SENT,
                SagaCommandTypes.VALIDATE_REQUEST, command))
                .isInstanceOf(SagaPublishException.class)
                .hasMessageContaining(SAGA_ID.toString())
                .hasMessageContaining(SagaCommandTypes.VALIDATE_REQUEST);

        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("Kafka send 실패 — SagaPublishException 으로 래핑")
    void kafkaFailureThrows() throws Exception {
        Object command = new Object();
        given(objectMapper.writeValueAsString(command)).willReturn(SERIALIZED);
        CompletableFuture<SendResult<String, String>> failed =
                CompletableFuture.failedFuture(new RuntimeException("broker down"));
        given(kafkaTemplate.send(any(ProducerRecord.class))).willReturn(failed);

        assertThatThrownBy(() -> publisher.publish(SAGA_ID, SagaSteps.CONFIRMED_ORDER_SENT,
                SagaCommandTypes.CONFIRMED_ORDER_REQUEST, command))
                .isInstanceOf(SagaPublishException.class)
                .hasMessageContaining(SAGA_ID.toString())
                .hasMessageContaining(SagaCommandTypes.CONFIRMED_ORDER_REQUEST);
    }

    private static String headerValue(ProducerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertThat(header).as("header %s", key).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
