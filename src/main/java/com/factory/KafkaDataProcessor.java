package com.factory;

import com.factory.kafka.config.model.EventsStreamsConfiguration;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.MeanStreamsConfiguration;
import com.factory.kafka.config.model.PerformanceStreamsConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
    KafkaNativeConfig.class,
    EventsStreamsConfiguration.class,
    MeanStreamsConfiguration.class,
    PerformanceStreamsConfiguration.class
})
public class KafkaDataProcessor {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDataProcessor.class, args);
    }

}
