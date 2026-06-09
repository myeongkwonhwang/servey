package io.github.orange2652.partner.channel.batch.external.toss;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 토스 주문 폴링 진입점 — @Scheduled.
 * 폴링 주기: fixedDelay 30s (이전 호출 종료 후 30s 대기).
 */
@Component
@RequiredArgsConstructor
class TossOrderPoller {

    private final TossOrderPollingService tossOrderPollingService;

    @Scheduled(fixedDelay = 30_000L)
    void poll() {
        tossOrderPollingService.poll();
    }
}
