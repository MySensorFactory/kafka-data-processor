package com.factory.kafka.config.model;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ConfigurationProperties("spring.kafka")
public class KafkaNativeConfig {

    private List<String> bootstrapServers;

    private String schemaRegistryUrl;

    private Boolean useSchemasLatestVersion = true;

    private Boolean autoRegisterSchemas = false;

    private Streams streams;

    private Consumer consumer;

    @Data
    public static class Streams {

        private String replicationFactor;

        private String applicationId;
    }

    @Data
    public static class Consumer {

        private String isolationLevel = "read_commited";

        private String autoOffsetReset = "latest";
    }

}
