package io.github.orange2652.partner.channel.adapter.external.toss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.client.toss.TossOrderStatuses;
import io.github.orange2652.partner.channel.common.Channel;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * A1 step 1 의 fast fail 기본 검증 — 외부 호출 가능성 + 데이터 무결성.
 *
 * <p><b>책임 (step 2 validate 와의 의도적 depth defense)</b></p>
 * <ul>
 *   <li>step 1 (여기, channel-adapter): <b>"외부 호출이 가능한 상태인가"</b> — 외부 API
 *       (PAID → PREPARING_PRODUCT 전이) 호출 전에 실패할 raw 를 거르는 fast fail. 잘못된 외부 호출 비용을 줄임.</li>
 *   <li>step 2 ({@code core.saga.validate.Validator}, service-core): <b>"판매 가능한가"</b> —
 *       비즈니스 룰. 현재는 PAID 동일 체크지만 재고 / 가격 / 채널 등록 등으로 확장될 자리.</li>
 * </ul>
 * <p>두 곳이 같은 PAID 룰을 보는 것은 우연이 아니라 <b>depth defense</b> — 외부 호출 직전과 비즈니스
 * 진입 직전에 각각 게이트가 있고, 룰이 향후 분기될 때 자연스럽게 갈라진다.</p>
 *
 * <p>현재 룰 (TOSS 한정):</p>
 * <ul>
 *   <li>{@code orderProductStatus == "PAID"} — 결제완료 상태의 주문만 적재 (PREPARING_PRODUCT 로 전이 가능한 상태)</li>
 *   <li>{@code orderId / orderProductId} 필수 필드</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class BasicValidator {

    private final ObjectMapper objectMapper;

    public Verdict validate(String channel, String raw) {
        if (!Channel.TOSS.code().equals(channel)) {
            return Verdict.failed("UNSUPPORTED_CHANNEL", "channel=" + channel);
        }
        JsonNode node;
        try {
            node = objectMapper.readTree(raw);
        } catch (JsonProcessingException e) {
            return Verdict.failed("RAW_PARSE_FAILED", e.getMessage());
        }
        if (!node.hasNonNull("orderId")) {
            return Verdict.failed("MISSING_ORDER_ID", "orderId is null/missing");
        }
        if (!node.hasNonNull("orderProductId")) {
            return Verdict.failed("MISSING_ORDER_PRODUCT_ID", "orderProductId is null/missing");
        }
        if (!node.hasNonNull("orderProductStatus")) {
            return Verdict.failed("MISSING_ORDER_STATUS", "orderProductStatus is null/missing");
        }
        String status = node.get("orderProductStatus").asText();
        if (!TossOrderStatuses.PAID.equals(status)) {
            return Verdict.failed("NOT_PAID", "orderProductStatus=" + status);
        }
        return Verdict.ok();
    }

    public record Verdict(boolean valid, String errorCode, String errorMessage) {

        public static Verdict ok() {
            return new Verdict(true, null, null);
        }

        public static Verdict failed(String errorCode, String errorMessage) {
            return new Verdict(false, errorCode, errorMessage);
        }
    }
}
