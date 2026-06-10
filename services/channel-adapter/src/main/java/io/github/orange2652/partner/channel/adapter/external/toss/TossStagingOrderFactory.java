package io.github.orange2652.partner.channel.adapter.external.toss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.orange2652.partner.channel.persistence.staging.domain.StagingOrder;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 토스 응답 raw JSON → {@link StagingOrder} 변환.
 *
 * <p>채널 특화 파서. 다른 채널은 별도 factory ({@code NaverStagingOrderFactory} 등) 로 분리.
 * {@code raw} 는 그대로 staging.raw 에 보존.</p>
 */
@Component
@RequiredArgsConstructor
public class TossStagingOrderFactory {

    static final String CHANNEL = "TOSS";

    private final ObjectMapper objectMapper;

    public StagingOrder from(String raw) throws JsonProcessingException {
        TossOrderPayload parsed = objectMapper.readValue(raw, TossOrderPayload.class);
        return StagingOrder.newRecord(
                CHANNEL,
                String.valueOf(parsed.orderId()),
                String.valueOf(parsed.orderProductId()),
                parsed.orderProductStatus(),
                parsed.orderedAt(),
                raw);
    }
}
