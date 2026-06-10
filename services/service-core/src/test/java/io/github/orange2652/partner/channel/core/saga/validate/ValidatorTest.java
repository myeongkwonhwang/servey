package io.github.orange2652.partner.channel.core.saga.validate;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.core.saga.validate.Validator.Verdict;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ValidatorTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        validator = new Validator(new ObjectMapper());
    }

    @Nested
    @DisplayName("TOSS 채널 룰")
    class Toss {

        @Test
        @DisplayName("orderProductStatus=PAID 면 sellable=true")
        void paidIsSellable() {
            String raw = "{\"orderProductId\":1,\"orderProductStatus\":\"PAID\"}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.sellable()).isTrue();
            assertThat(verdict.errorCode()).isNull();
        }

        @Test
        @DisplayName("orderProductStatus 가 PAID 이외이면 FAIL — NOT_SELLABLE")
        void nonPaidIsNotSellable() {
            String raw = "{\"orderProductId\":1,\"orderProductStatus\":\"CANCELED\"}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.sellable()).isFalse();
            assertThat(verdict.errorCode()).isEqualTo("NOT_SELLABLE");
            assertThat(verdict.errorMessage()).contains("CANCELED");
        }

        @Test
        @DisplayName("orderProductStatus 필드 누락 시 NOT_SELLABLE")
        void missingStatusIsNotSellable() {
            String raw = "{\"orderProductId\":1}";

            Verdict verdict = validator.validate("TOSS", raw);

            assertThat(verdict.sellable()).isFalse();
            assertThat(verdict.errorCode()).isEqualTo("NOT_SELLABLE");
            assertThat(verdict.errorMessage()).contains("missing");
        }

        @Test
        @DisplayName("raw 가 JSON 파싱 실패면 RAW_PARSE_FAILED")
        void invalidJsonFails() {
            Verdict verdict = validator.validate("TOSS", "not-a-json");

            assertThat(verdict.sellable()).isFalse();
            assertThat(verdict.errorCode()).isEqualTo("RAW_PARSE_FAILED");
        }
    }

    @Nested
    @DisplayName("미지원 채널")
    class UnsupportedChannel {

        @Test
        @DisplayName("TOSS 가 아닌 채널은 UNSUPPORTED_CHANNEL 로 FAIL")
        void naverFails() {
            Verdict verdict = validator.validate("NAVER", "{\"orderProductStatus\":\"PAID\"}");

            assertThat(verdict.sellable()).isFalse();
            assertThat(verdict.errorCode()).isEqualTo("UNSUPPORTED_CHANNEL");
            assertThat(verdict.errorMessage()).contains("NAVER");
        }
    }
}
