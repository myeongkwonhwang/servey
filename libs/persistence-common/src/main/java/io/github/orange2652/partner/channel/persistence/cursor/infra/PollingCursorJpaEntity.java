package io.github.orange2652.partner.channel.persistence.cursor.infra;

import io.github.orange2652.partner.channel.persistence.cursor.domain.PollingCursor;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "polling_cursor", schema = "channel_schema")
@IdClass(PollingCursorId.class)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
class PollingCursorJpaEntity {

    @Id
    @Column(name = "channel", nullable = false, length = 32)
    private String channel;

    @Id
    @Column(name = "resource", nullable = false, length = 32)
    private String resource;

    @Column(name = "window_start", nullable = false)
    private LocalDateTime windowStart;

    @Column(name = "window_end", nullable = false)
    private LocalDateTime windowEnd;

    @Column(name = "next_cursor", columnDefinition = "text")
    private String nextCursor;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    static PollingCursorJpaEntity from(PollingCursor cursor) {
        return PollingCursorJpaEntity.builder()
                .channel(cursor.channel())
                .resource(cursor.resource())
                .windowStart(cursor.windowStart())
                .windowEnd(cursor.windowEnd())
                .nextCursor(cursor.nextCursor())
                .updatedAt(cursor.updatedAt())
                .build();
    }

    PollingCursor toDomain() {
        return new PollingCursor(channel, resource, windowStart, windowEnd, nextCursor, updatedAt);
    }
}
