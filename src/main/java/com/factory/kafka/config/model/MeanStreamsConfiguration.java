package com.factory.kafka.config.model;

import lombok.Data;

import java.util.Map;

@Data
public class MeanStreamsConfiguration {
    private Map<String, MeanStreamConfig> mean;
}
