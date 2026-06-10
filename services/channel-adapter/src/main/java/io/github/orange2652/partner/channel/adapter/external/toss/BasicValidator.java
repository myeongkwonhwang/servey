package io.github.orange2652.partner.channel.adapter.external.toss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.client.toss.TossOrderStatuses;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * A1 step 1 의 fast fail 기본 검증 — 데이터 무결성.
 *
 * <p>step 2 validate (비즈니스 검증, service-core) 와 책임이 다름:</p>
 * <ul>
 *   <li>기본검증 (여기): 필수 필드 / 외부 상태가 적재 가능한지 (raw 가 신뢰 못 할 데이터일 수 있음)</li>
 *   <li>비즈니스 검증 (service-core): 재고 / 가격 / 판매가능 — saga step 2</li>
 * </ul>
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
        if (!"TOSS".equals(channel)) {
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
