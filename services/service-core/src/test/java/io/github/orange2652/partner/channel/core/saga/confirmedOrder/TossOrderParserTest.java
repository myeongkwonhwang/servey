package io.github.orange2652.partner.channel.core.saga.confirmedOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.core.saga.confirmedOrder.TossOrderParser.Parsed;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TossOrderParserTest {

    private TossOrderParser parser;

    @BeforeEach
    void setUp() {
        parser = new TossOrderParser(new ObjectMapper());
    }

    @Test
    @DisplayName("정상 raw → externalOrderId / externalOrderProductId 추출")
    void parseExtractsIds() throws JsonProcessingException {
        String raw = "{\"orderId\":12345,\"orderProductId\":67890,\"orderProductStatus\":\"PAID\"}";

        Parsed parsed = parser.parse(raw);

        assertThat(parsed.externalOrderId()).isEqualTo("12345");
        assertThat(parsed.externalOrderProductId()).isEqualTo("67890");
    }

    @Test
    @DisplayName("invalid JSON → JsonProcessingException")
    void invalidJsonThrows() {
        assertThatThrownBy(() -> parser.parse("not-a-json"))
                .isInstanceOf(JsonProcessingException.class);
    }
}
