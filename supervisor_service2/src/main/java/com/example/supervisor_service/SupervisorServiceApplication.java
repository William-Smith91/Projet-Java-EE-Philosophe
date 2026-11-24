package com.example.supervisor_service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SupervisorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SupervisorServiceApplication.class, args);
    }

    @Bean
    public ForkTable forkTable(@Value("${supervisor.fork-count}") int forkCount,
                               @Value("${supervisor.check-to-act-delay-ms}") long delayMs) {
        return new ForkTable(forkCount, delayMs);
    }
}
