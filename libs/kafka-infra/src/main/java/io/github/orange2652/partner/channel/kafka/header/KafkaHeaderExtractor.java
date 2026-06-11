package io.github.orange2652.partner.channel.kafka.header;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * Kafka header (byte[]) 추출 유틸. 각 Consumer 에서 반복되던 null 체크 / charset 변환 /
 * UUID 파싱 보일러플레이트를 제거.
 *
 * <p>의도적으로 {@code null} 을 빈 값 (또는 {@link Optional#empty()}) 으로 흡수한다 — 헤더 누락은
 * 처리 흐름상 "skip" 으로 다뤄지므로 호출자가 명시적 if/return 으로 분기하기 좋도록.</p>
 */
public final class KafkaHeaderExtractor {

    private KafkaHeaderExtractor() {
    }

    /**
     * @return {@code header} 가 null 이면 빈 문자열, 아니면 UTF-8 디코드 결과.
     */
    public static String stringOrEmpty(byte[] header) {
        return header == null ? "" : new String(header, StandardCharsets.UTF_8);
    }

    /**
     * @return UUID 로 파싱 가능하면 {@code Optional.of}, null / blank / non-UUID 면 empty.
     */
    public static Optional<UUID> uuid(byte[] header) {
        if (header == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(UUID.fromString(new String(header, StandardCharsets.UTF_8)));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
