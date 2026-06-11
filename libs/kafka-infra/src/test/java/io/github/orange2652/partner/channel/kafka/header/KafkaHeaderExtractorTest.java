package io.github.orange2652.partner.channel.kafka.header;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KafkaHeaderExtractorTest {

    @Test
    @DisplayName("stringOrEmpty — null 은 빈 문자열")
    void stringNullToEmpty() {
        assertThat(KafkaHeaderExtractor.stringOrEmpty(null)).isEmpty();
    }

    @Test
    @DisplayName("stringOrEmpty — byte[] UTF-8 디코드")
    void stringDecodesUtf8() {
        byte[] header = "VALIDATE_REQUEST".getBytes(StandardCharsets.UTF_8);
        assertThat(KafkaHeaderExtractor.stringOrEmpty(header)).isEqualTo("VALIDATE_REQUEST");
    }

    @Test
    @DisplayName("uuid — null 은 Optional.empty")
    void uuidNullToEmpty() {
        assertThat(KafkaHeaderExtractor.uuid(null)).isEmpty();
    }

    @Test
    @DisplayName("uuid — UUID 가 아니면 Optional.empty")
    void uuidInvalidToEmpty() {
        byte[] header = "not-a-uuid".getBytes(StandardCharsets.UTF_8);
        assertThat(KafkaHeaderExtractor.uuid(header)).isEmpty();
    }

    @Test
    @DisplayName("uuid — 유효한 UUID 디코드")
    void uuidParses() {
        UUID expected = UUID.fromString("11111111-2222-3333-4444-555555555555");
        byte[] header = expected.toString().getBytes(StandardCharsets.UTF_8);

        Optional<UUID> actual = KafkaHeaderExtractor.uuid(header);

        assertThat(actual).contains(expected);
    }
}
