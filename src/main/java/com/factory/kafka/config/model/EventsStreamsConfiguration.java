package com.factory.kafka.config.model;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("spring.kafka.streams.config.events")
public class EventsStreamsConfiguration {
    private EventsStreamConfig temperature;
    private EventsStreamConfig pressure;
    private EventsStreamConfig flowRate;
    private EventsStreamConfig gasComposition;
    private EventsStreamConfig noiseAndVibration;
}