package io.github.orange2652.partner.channel.adapter.external.toss;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.adapter.external.toss.BasicValidator.Verdict;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BasicValidatorTest {

    private BasicValidator validator;

    @BeforeEach
    void setUp() {
        validator = new BasicValidator(new ObjectMapper());
    }

    @Nested
    @DisplayName("TOSS — 데이터 무결성 체크")
    class TossIntegrity {

        @Test
        @DisplayName("orderProductStatus=PAID + 필수 필드 → OK")
        void paidIsOk() {
            String raw = "{\"orderId\":1,\"orderProductId\":2,\"orderProductStatus\":\"PAID\"}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.valid()).isTrue();
            assertThat(verdict.errorCode()).isNull();
        }

        @Test
        @DisplayName("orderProductStatus != PAID → NOT_PAID")
        void nonPaidFails() {
            String raw = "{\"orderId\":1,\"orderProductId\":2,\"orderProductStatus\":\"PREPARING_PRODUCT\"}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.valid()).isFalse();
            assertThat(verdict.errorCode()).isEqualTo("NOT_PAID");
            assertThat(verdict.errorMessage()).contains("PREPARING_PRODUCT");
        }

        @Test
        @DisplayName("orderId 누락 → MISSING_ORDER_ID")
        void missingOrderIdFails() {
            String raw = "{\"orderProductId\":2,\"orderProductStatus\":\"PAID\"}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.errorCode()).isEqualTo("MISSING_ORDER_ID");
        }

        @Test
        @DisplayName("orderProductId 누락 → MISSING_ORDER_PRODUCT_ID")
        void missingOrderProductIdFails() {
            String raw = "{\"orderId\":1,\"orderProductStatus\":\"PAID\"}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.errorCode()).isEqualTo("MISSING_ORDER_PRODUCT_ID");
        }

        @Test
        @DisplayName("orderProductStatus 누락 → MISSING_ORDER_STATUS")
        void missingStatusFails() {
            String raw = "{\"orderId\":1,\"orderProductId\":2}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.errorCode()).isEqualTo("MISSING_ORDER_STATUS");
        }

        @Test
        @DisplayName("invalid JSON → RAW_PARSE_FAILED")
        void invalidJsonFails() {
            Verdict verdict = validator.validate("TOSS", "not-a-json");

            assertThat(verdict.errorCode()).isEqualTo("RAW_PARSE_FAILED");
        }
    }

    @Nested
    @DisplayName("미지원 채널")
    class UnsupportedChannel {

        @Test
        @DisplayName("TOSS 가 아닌 채널 → UNSUPPORTED_CHANNEL")
        void naverFails() {
            Verdict verdict = validator.validate("NAVER", "{\"orderProductStatus\":\"PAID\"}");

            assertThat(verdict.errorCode()).isEqualTo("UNSUPPORTED_CHANNEL");
        }
    }
}
