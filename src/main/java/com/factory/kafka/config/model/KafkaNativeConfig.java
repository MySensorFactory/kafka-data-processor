package com.factory.kafka.config.model;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class KafkaNativeConfig {
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.schemaRegistryUrl}")
    private String schemaRegistryUrl;

    @Value(value = "${spring.kafka.streams.replication-factor}")
    private String streamsReplicationFactor;

    @Value(value = "${spring.kafka.streams.application-id}")
    private String applicationId;

    @Value(value = "${spring.kafka.useSchemasLatestVersion:true}")
    private Boolean useSchemasLatestVersion;

    @Value(value = "${spring.kafka.autoRegisterSchemas:false}")
    private Boolean autoRegisterSchemas;

    @Value(value = "${spring.kafka.consumer.isolation-level:read_commited}")
    private String isolationLevel;

    @Value(value = "${spring.kafka.consumer.auto-offset-reset:latest}")
    private String autoOffsetReset;
}
