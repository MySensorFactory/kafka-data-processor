package com.factory.kafka.config.model;

import lombok.Data;

@Data
public class PerformanceStreamConfig {
    private Integer joinWindowsDurationMillis;
    private String inputTopic;
    private String outputTopic;
    private String resultTopic;
    private Boolean debugEnabled;
}
