package com.factory.kafka.config.model;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@ConfigurationProperties("spring.kafka.streams.config")
public class MeanStreamsConfiguration {
    private Map<String, MeanStreamConfig> mean;
}
