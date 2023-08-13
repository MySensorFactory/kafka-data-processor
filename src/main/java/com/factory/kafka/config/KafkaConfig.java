package com.factory.kafka.config;

import lombok.Data;

@Data
public class KafkaConfig {
    private String temperatureTopicName;
    private String pressureTopicName;
    private String resultTopic;
}
