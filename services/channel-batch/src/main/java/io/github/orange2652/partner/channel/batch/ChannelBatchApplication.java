package io.github.orange2652.partner.channel.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "io.github.orange2652.partner.channel")
@EntityScan(basePackages = "io.github.orange2652.partner.channel")
@EnableJpaRepositories(basePackages = "io.github.orange2652.partner.channel")
@EnableScheduling
public class ChannelBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChannelBatchApplication.class, args);
    }
}
