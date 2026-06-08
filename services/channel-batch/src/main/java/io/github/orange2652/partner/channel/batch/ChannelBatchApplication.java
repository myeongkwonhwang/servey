package io.github.orange2652.partner.channel.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ChannelBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChannelBatchApplication.class, args);
    }
}
