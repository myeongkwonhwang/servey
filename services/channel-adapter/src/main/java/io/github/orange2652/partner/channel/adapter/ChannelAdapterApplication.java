package io.github.orange2652.partner.channel.adapter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = "io.github.orange2652.partner.channel")
@EntityScan(basePackages = "io.github.orange2652.partner.channel")
@EnableJpaRepositories(basePackages = "io.github.orange2652.partner.channel")
public class ChannelAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChannelAdapterApplication.class, args);
    }
}
