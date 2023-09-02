package com.factory.kafka.config;

import com.factory.kafka.config.factory.PressureMeanStreamFactory;
import com.factory.kafka.config.factory.TemperatureMeanStreamFactory;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.StreamsConfiguration;
import com.factory.message.Pressure;
import com.factory.message.Temperature;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class FactoryKafkaStreamsConfiguration {

    @Bean
    @ConfigurationProperties("spring.kafka.streams")
    public StreamsConfiguration streamsConfig() {
        return new StreamsConfiguration();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(final KafkaNativeConfig kafkaNativeConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, kafkaNativeConfig.getApplicationId());
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaNativeConfig.getBootstrapAddress());

        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());

        props.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaNativeConfig.getSchemaRegistryUrl());

        props.put(REPLICATION_FACTOR_CONFIG, kafkaNativeConfig.getStreamsReplicationFactor());
        props.put(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> log.info("State transition from " + oldState + " to " + newState));
    }

    @Bean
    public PressureMeanStreamFactory pressureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                               final StreamsConfiguration streamsConfiguration) {
        final var pressureConfigurationKey = "pressure";
        final var config = streamsConfiguration.getConfig().get(pressureConfigurationKey);
        return PressureMeanStreamFactory.builder()
                .kafkaNativeConfig(kafkaNativeConfig)
                .config(config)
                .build();
    }

    @Bean
    public List<KStream<String, Pressure>> pressureMeanStreams(final StreamsBuilder streamsBuilder,
                                                              final PressureMeanStreamFactory factory) {
        return factory.splitToMeanPressureBranches(streamsBuilder);
    }

    @Bean
    public TemperatureMeanStreamFactory temperatureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                                  final StreamsConfiguration streamsConfiguration) {
        final var pressureConfigurationKey = "temperature";
        final var config = streamsConfiguration.getConfig().get(pressureConfigurationKey);
        return TemperatureMeanStreamFactory.builder()
                .kafkaNativeConfig(kafkaNativeConfig)
                .config(config)
                .build();
    }

    @Bean
    public List<KStream<String, Temperature>> temperatureMeanStreams(final StreamsBuilder streamsBuilder,
                                                                  final TemperatureMeanStreamFactory factory) {
        return factory.splitToMeanTemperatureBranches(streamsBuilder);
    }
}
