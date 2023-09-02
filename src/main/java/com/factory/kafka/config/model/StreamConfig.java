package com.factory.kafka.config.model;

import lombok.Data;

import java.util.List;

@Data
public class StreamConfig {
    private String inputTopic;
    private List<String> labels;
    private Integer windowSize;
    private Boolean debugEnabled;
    private String outputTopicsPostfix;
}
