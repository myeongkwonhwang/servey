package io.github.orange2652.partner.channel.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Saga Orchestrator — A1 흐름의 saga 인스턴스 lifecycle 을 관리.
 *
 * <p>Scan 범위를 부모 패키지 {@code io.github.orange2652.partner.channel} 으로 확장하여
 * {@code libs/persistence-common} 의 {@code @Component} / {@code @Entity} /
 * Spring Data JPA repository 가 모두 잡히도록 함.</p>
 */
@SpringBootApplication(scanBasePackages = "io.github.orange2652.partner.channel")
@EntityScan(basePackages = "io.github.orange2652.partner.channel")
@EnableJpaRepositories(basePackages = "io.github.orange2652.partner.channel")
public class SagaOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaOrchestratorApplication.class, args);
    }
}
