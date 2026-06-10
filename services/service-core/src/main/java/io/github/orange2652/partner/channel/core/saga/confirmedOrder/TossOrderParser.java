package io.github.orange2652.partner.channel.core.saga.confirmedOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * TOSS 응답 raw JSON → 정규화 식별자 추출.
 *
 * <p>step 3 (confirmedOrder) 에서 service-core 가 외부 물류 호출 + 내부 주문 INSERT 위해 필요한
 * {@code externalOrderId / externalOrderProductId} 만 추출. 다른 채널은 별도 parser 추가.</p>
 */
@Component
@RequiredArgsConstructor
class TossOrderParser {

    private final ObjectMapper objectMapper;

    Parsed parse(String raw) throws JsonProcessingException {
        JsonNode node = objectMapper.readTree(raw);
        return new Parsed(
                node.get("orderId").asText(),
                node.get("orderProductId").asText());
    }

    record Parsed(String externalOrderId, String externalOrderProductId) {
    }
}
