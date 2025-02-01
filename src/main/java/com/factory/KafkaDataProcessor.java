package com.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaNativeConfig.class)
public class KafkaDataProcessor {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDataProcessor.class, args);
    }

}
