package io.github.orange2652.partner.channel.persistence.cursor.infra;

import java.io.Serializable;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
class PollingCursorId implements Serializable {

    private String channel;
    private String resource;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PollingCursorId that)) return false;
        return Objects.equals(channel, that.channel) && Objects.equals(resource, that.resource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channel, resource);
    }
}
