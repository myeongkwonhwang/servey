package io.github.orange2652.partner.channel.batch.publisher;

import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Outbox event 의 (aggregateType, eventType) → Kafka 토픽 매핑.
 *
 * 향후 진화:
 *   - aggregateType 추가 시 TOPICS 에 한 줄
 *   - 외부 설정 (application.yml) 로 옮길 수 있음
 *   - type 별 별도 Publisher 분리 (다중 publisher) 시 router 인터페이스화 후 type 별 구현
 */
@Component
class OutboxTopicRouter {

    private static final Map<String, String> TOPICS = Map.of(
            "ORDER", "channel.order.received"
            // future: "CLAIM", "channel.claim.received"
    );

    String resolve(String aggregateType, String eventType) {
        String topic = TOPICS.get(aggregateType);
        if (topic == null) {
            throw new IllegalStateException(
                    "no topic mapping aggregateType=" + aggregateType + " eventType=" + eventType);
        }
        return topic;
    }
}
