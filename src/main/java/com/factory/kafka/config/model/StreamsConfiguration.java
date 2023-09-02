package com.factory.kafka.config.model;

import lombok.Data;

import java.util.Map;

@Data
public class StreamsConfiguration {
    private Map<String, StreamConfig> config;
}
