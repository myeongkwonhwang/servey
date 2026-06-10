package io.github.orange2652.partner.channel.core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Service Core — A1 step 2 validate / step 3 confirmedOrder (Pivot) / B1 sendInvoice 처리.
 *
 * <p>Scan 범위를 부모 패키지 {@code io.github.orange2652.partner.channel} 으로 확장.</p>
 */
@SpringBootApplication(scanBasePackages = "io.github.orange2652.partner.channel")
@EntityScan(basePackages = "io.github.orange2652.partner.channel")
@EnableJpaRepositories(basePackages = "io.github.orange2652.partner.channel")
public class ServiceCoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServiceCoreApplication.class, args);
    }
}
