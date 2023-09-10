package com.factory.kafka.config.model;

import lombok.Data;

import java.util.Map;

@Data
public class PerformanceStreamsConfiguration {
    private Map<String, PerformanceStreamConfig> performance;
}
