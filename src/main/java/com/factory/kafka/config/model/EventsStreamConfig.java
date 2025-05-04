package com.factory.kafka.config.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class EventsStreamConfig {
    private Boolean enabled;
    private String inputTopic;
    private String outputTopic;
    private Map<String, SensorThresholds> thresholds;
    private Boolean debugEnabled;
    private Long cooldownPeriodMs;  // Cooldown period between successive alerts in milliseconds

    @Data
    public static class SensorThresholds {
        private Double warningThreshold;
        private Double criticalThreshold;
    }
}