package com.factory.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class FactoryKafkaStreamsConfiguration {

    @Bean
    @ConfigurationProperties("kafka")
    public KafkaConfig kafkaConfig() {
        return new KafkaConfig();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(final KafkaNativeConfig kafkaNativeConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FactoryStreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNativeConfig.getBootstrapAddress());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> log.info("State transition from " + oldState + " to " + newState));
    }

    @Bean
    public KStream<String, Long> kStream(final StreamsBuilder kStreamBuilder, final KafkaConfig kafkaConfig) {
        KStream<String, String> stream = kStreamBuilder.stream(kafkaConfig.getPressureTopicName());
        var resultStream = stream
                .flatMapValues(value -> value.toLowerCase().chars()
                        .filter(Character::isLetter)
                        .mapToObj(c -> (char) c)
                        .collect(Collectors.toList()))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(12)))
                .count()
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), count));

        resultStream.to(kafkaConfig().getResultTopic(), Produced.with(Serdes.String(), Serdes.Long()));
        resultStream.print(Printed.toSysOut());

        return resultStream;
    }
}
