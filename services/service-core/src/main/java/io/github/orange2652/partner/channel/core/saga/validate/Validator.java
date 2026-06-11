package io.github.orange2652.partner.channel.core.saga.validate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.common.Channel;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * A1 step 2 — 판매가능 비즈니스 검증 (read-only).
 *
 * <p><b>책임 (step 1 BasicValidator 와의 의도적 depth defense)</b></p>
 * <ul>
 *   <li>step 1 ({@code channel-adapter.BasicValidator}): "외부 호출 가능한가" — fast fail 데이터 무결성.</li>
 *   <li>step 2 (여기, service-core): <b>"판매 가능한가"</b> — 비즈니스 룰. 재고 / 가격 / 채널 등록 /
 *       상품 활성화 등으로 확장될 자리.</li>
 * </ul>
 * <p>현재 둘 다 PAID 만 보고 있어 표면상 중복이지만, 검증 레이어/목적이 다르므로 룰 확장 시 자연스럽게
 * 갈라진다 (예: step 2 가 채널 등록 / 재고로 확장돼도 step 1 은 raw 무결성에 집중).</p>
 *
 * <p>채널별 룰:</p>
 * <ul>
 *   <li>{@code TOSS} — raw 의 {@code orderProductStatus == "PAID"} 만 통과</li>
 *   <li>그 외 채널 — 미지원 → {@link Verdict#unsupportedChannel} 으로 FAIL</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class Validator {

    static final String TOSS_SELLABLE_STATUS = "PAID";

    private final ObjectMapper objectMapper;

    public Verdict validate(String channel, String raw) {
        if (!Channel.TOSS.code().equals(channel)) {
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
