package io.github.orange2652.partner.channel.core.saga.validate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * A1 step 2 — 판매가능 검증 (read-only).
 *
 * <p>채널별 룰:</p>
 * <ul>
 *   <li>{@code TOSS} — raw 의 {@code orderProductStatus == "PAID"} 만 통과</li>
 *   <li>그 외 채널 — 미지원 → {@link Verdict#unsupportedChannel} 으로 FAIL</li>
 * </ul>
 *
 * <p>학습 단계 단순 룰. 실 환경에서는 상품 활성화 / 재고 / 채널 등록 / 가격 등 다항 검증.</p>
 */
@Component
@RequiredArgsConstructor
public class Validator {

    static final String CHANNEL_TOSS = "TOSS";
    static final String TOSS_SELLABLE_STATUS = "PAID";

    private final ObjectMapper objectMapper;

    public Verdict validate(String channel, String raw) {
        if (!CHANNEL_TOSS.equals(channel)) {
            return Verdict.unsupportedChannel(channel);
        }
        JsonNode node;
        try {
            node = objectMapper.readTree(raw);
        } catch (JsonProcessingException e) {
            return Verdict.failed("RAW_PARSE_FAILED", e.getMessage());
        }
        if (node.hasNonNull("orderProductStatus")
                && TOSS_SELLABLE_STATUS.equals(node.get("orderProductStatus").asText())) {
            return Verdict.ok();
        }
        String status = node.hasNonNull("orderProductStatus")
                ? node.get("orderProductStatus").asText() : "(missing)";
        return Verdict.failed("NOT_SELLABLE", "orderProductStatus=" + status);
    }

    public record Verdict(boolean sellable, String errorCode, String errorMessage) {

        public static Verdict ok() {
            return new Verdict(true, null, null);
        }

        public static Verdict failed(String errorCode, String errorMessage) {
            return new Verdict(false, errorCode, errorMessage);
        }

        public static Verdict unsupportedChannel(String channel) {
            return new Verdict(false, "UNSUPPORTED_CHANNEL", "channel=" + channel);
        }
    }
}
