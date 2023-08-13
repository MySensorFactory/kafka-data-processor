package com.factory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaDataProcessor {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDataProcessor.class, args);
    }

}
