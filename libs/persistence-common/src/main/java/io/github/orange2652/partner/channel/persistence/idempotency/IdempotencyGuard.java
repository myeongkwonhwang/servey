package io.github.orange2652.partner.channel.persistence.idempotency;

import io.github.orange2652.partner.channel.persistence.idempotency.domain.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Kafka 메시지 멱등성 체크 wrapper — eventId 형식 ("partition:offset") + repository 조회를
 * 한 곳에 모은다.
 *
 * <p>각 Consumer 가 동일 패턴을 반복하던 것을 한 곳으로 모으되, processed_event INSERT 는
 * 호출자의 Tx 안에서 처리하도록 책임 분리. 본 wrapper 는 read-only 체크만.</p>
 */
@Component
@RequiredArgsConstructor
public class IdempotencyGuard {

    private final ProcessedEventRepository processedEventRepository;

    /**
     * eventId 형식 빌더. Kafka 메시지 위치 (partition + offset) 로 고유 키 생성.
     * 향후 형식 변경 (예: {@code "topic:partition:offset"}) 시 본 메서드만 수정.
     */
    public static String eventId(int partition, long offset) {
        return partition + ":" + offset;
    }

    /**
     * @return 이미 처리된 (consumer, eventId) 면 true.
     */
    public boolean isAlreadyProcessed(String consumerName, String eventId) {
        return processedEventRepository.exists(consumerName, eventId);
    }
}
