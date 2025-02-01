package com.factory.kafka.config;

import com.factory.kafka.config.factory.FlowRateMeanStreamFactory;
import com.factory.kafka.config.factory.GasCompositionMeanStreamFactory;
import com.factory.kafka.config.factory.NoiseAndVibrationMeanStreamFactory;
import com.factory.kafka.config.factory.PressureMeanStreamFactory;
import com.factory.kafka.config.factory.ReactionPerformanceStreamFactory;
import com.factory.kafka.config.factory.TemperatureMeanStreamFactory;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.MeanStreamsConfiguration;
import com.factory.kafka.config.model.PerformanceStreamsConfiguration;
import com.factory.message.FlowRate;
import com.factory.message.GasComposition;
import com.factory.message.NoiseAndVibration;
import com.factory.message.Performance;
import com.factory.message.Pressure;
import com.factory.message.Temperature;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class FactoryKafkaStreamsConfiguration {

    @Bean
    @ConfigurationProperties("spring.kafka.streams.config")
    public MeanStreamsConfiguration meanStreamsConfiguration() {
        return new MeanStreamsConfiguration();
    }

    @Bean
    @ConfigurationProperties("spring.kafka.streams.config")
    public PerformanceStreamsConfiguration performanceStreamsConfiguration() {
        return new PerformanceStreamsConfiguration();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(final KafkaNativeConfig kafkaNativeConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, kafkaNativeConfig.getStreams().getApplicationId());
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaNativeConfig.getBootstrapServers());

        props.put(AUTO_OFFSET_RESET_CONFIG, kafkaNativeConfig.getConsumer().getAutoOffsetReset());
        props.put(ISOLATION_LEVEL_CONFIG, kafkaNativeConfig.getConsumer().getIsolationLevel());

        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put(SCHEMA_REGISTRY_URL_CONFIG, kafkaNativeConfig.getSchemaRegistryUrl());
        props.put(AUTO_REGISTER_SCHEMAS, kafkaNativeConfig.getAutoRegisterSchemas());
        props.put(USE_LATEST_VERSION, kafkaNativeConfig.getUseSchemasLatestVersion());
        props.put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

        props.put(REPLICATION_FACTOR_CONFIG, kafkaNativeConfig.getStreams().getReplicationFactor());
        props.put(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        props.put(PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> log.info("State transition from " + oldState + " to " + newState));
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.kafka.streams.config.mean.pressure.enabled",
            havingValue = "true")
    public PressureMeanStreamFactory pressureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                               final MeanStreamsConfiguration meanStreamsConfiguration) {
        final var pressureConfigurationKey = "pressure";
        final var config = meanStreamsConfiguration.getMean().get(pressureConfigurationKey);
        return PressureMeanStreamFactory.builder()
                .kafkaNativeConfig(kafkaNativeConfig)
                .config(config)
                .build();
    }

    @Bean
    @ConditionalOnBean(PressureMeanStreamFactory.class)
    public List<KStream<String, Pressure>> pressureMeanStreams(final StreamsBuilder streamsBuilder,
                                                               final PressureMeanStreamFactory factory) {
        return factory.splitToMeanBranches(streamsBuilder);
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.kafka.streams.config.mean.temperature.enabled",
            havingValue = "true")
    public TemperatureMeanStreamFactory temperatureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                                     final MeanStreamsConfiguration meanStreamsConfiguration) {
        final var pressureConfigurationKey = "temperature";
        final var config = meanStreamsConfiguration.getMean().get(pressureConfigurationKey);
        return TemperatureMeanStreamFactory.builder()
                .kafkaNativeConfig(kafkaNativeConfig)
                .config(config)
                .build();
    }

    @Bean
    @ConditionalOnBean(TemperatureMeanStreamFactory.class)
    public List<KStream<String, Temperature>> temperatureMeanStreams(final StreamsBuilder streamsBuilder,
                                                                     final TemperatureMeanStreamFactory factory) {
        return factory.splitToMeanBranches(streamsBuilder);
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.kafka.streams.config.mean.flowRate.enabled",
            havingValue = "true")
    public FlowRateMeanStreamFactory flowRateMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                               final MeanStreamsConfiguration meanStreamsConfiguration) {
        final var pressureConfigurationKey = "flowRate";
        final var config = meanStreamsConfiguration.getMean().get(pressureConfigurationKey);
        return FlowRateMeanStreamFactory.builder()
                .kafkaNativeConfig(kafkaNativeConfig)
                .config(config)
                .build();
    }

    @Bean
    @ConditionalOnBean(FlowRateMeanStreamFactory.class)
    public List<KStream<String, FlowRate>> flowRateMeanStreams(final StreamsBuilder streamsBuilder,
                                                               final FlowRateMeanStreamFactory factory) {
        return factory.splitToMeanBranches(streamsBuilder);
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.kafka.streams.config.mean.gasComposition.enabled",
            havingValue = "true")
    public GasCompositionMeanStreamFactory gasCompositionMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                                           final MeanStreamsConfiguration meanStreamsConfiguration) {
        final var pressureConfigurationKey = "gasComposition";
        final var config = meanStreamsConfiguration.getMean().get(pressureConfigurationKey);
        return GasCompositionMeanStreamFactory.builder()
                .kafkaNativeConfig(kafkaNativeConfig)
                .config(config)
                .build();
    }

    @Bean
    @ConditionalOnBean(GasCompositionMeanStreamFactory.class)
    public List<KStream<String, GasComposition>> gasCompositionMeanStreams(final StreamsBuilder streamsBuilder,
                                                                           final GasCompositionMeanStreamFactory factory) {
        return factory.splitToMeanBranches(streamsBuilder);
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.kafka.streams.config.mean.noiseAndVibration.enabled",
            havingValue = "true")
    public NoiseAndVibrationMeanStreamFactory noiseAndVibrationMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                                                 final MeanStreamsConfiguration meanStreamsConfiguration) {
        final var pressureConfigurationKey = "noiseAndVibration";
        final var config = meanStreamsConfiguration.getMean().get(pressureConfigurationKey);
        return NoiseAndVibrationMeanStreamFactory.builder()
                .kafkaNativeConfig(kafkaNativeConfig)
                .config(config)
                .build();
    }

    @Bean
    @ConditionalOnBean(NoiseAndVibrationMeanStreamFactory.class)
    public List<KStream<String, NoiseAndVibration>> noiseAndVibrationMeanStreams(final StreamsBuilder streamsBuilder,
                                                                                 final NoiseAndVibrationMeanStreamFactory factory) {
        return factory.splitToMeanBranches(streamsBuilder);
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.kafka.streams.config.performance.reaction.enabled",
            havingValue = "true")
    public ReactionPerformanceStreamFactory reactionPerformanceStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                                                             final PerformanceStreamsConfiguration streamsConfiguration) {
        final var configurationKey = "reaction";
        final var config = streamsConfiguration.getPerformance().get(configurationKey);
        return ReactionPerformanceStreamFactory.builder()
                .performanceStreamConfig(config)
                .kafkaNativeConfig(kafkaNativeConfig)
                .build();
    }

    @Bean
    @ConditionalOnBean(ReactionPerformanceStreamFactory.class)
    public KStream<String, Performance> reactionPerformanceStream(final StreamsBuilder streamsBuilder,
                                                                  final ReactionPerformanceStreamFactory factory) {
        return factory.createPerformanceStream(streamsBuilder);
    }

}
