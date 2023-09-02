package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.StreamConfig;
import com.factory.message.Temperature;
import com.factory.message.TemperatureAggregation;
import com.factory.message.TemperatureDataRecord;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Builder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;

public class TemperatureMeanStreamFactory {
    public static final float INITIAL_VALUE = 0;
    private final int windowSize;
    private final List<String> labels;
    private final boolean debugEnabled;
    private final String outputTopicsPostfix;
    private final String inputTopic;
    private final String schemaRegistryUrl;
    private final Serde<Temperature> temperatureSerde;
    private final Serde<TemperatureAggregation> temperatureAggregationSerde;
    private final Predicate[] predicates;

    @Builder
    public TemperatureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                        final StreamConfig config) {
        this.windowSize = config.getWindowSize();
        this.labels = config.getLabels();
        this.debugEnabled = config.getDebugEnabled();
        this.outputTopicsPostfix = config.getOutputTopicsPostfix();
        this.inputTopic = config.getInputTopic();
        this.schemaRegistryUrl = kafkaNativeConfig.getSchemaRegistryUrl();
        this.temperatureSerde = new SpecificAvroSerde<>();
        temperatureSerde.configure(
                singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                , false);
        this.temperatureAggregationSerde = new SpecificAvroSerde<>();
        temperatureAggregationSerde.configure(
                singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                , false);
        this.predicates = labels.stream()
                .map(label -> (Predicate<String, Temperature>) (key, value) -> value.getLabel().toString().equals(label))
                .toArray(Predicate[]::new);
    }

    public List<KStream<String, Temperature>> splitToPredicatedBranches(final KStream<String, Temperature> inputStream) {
        final KStream<String, Temperature>[] result = inputStream.branch(predicates);
        return Arrays.stream(result).toList();

    }

    public List<KStream<String, Temperature>> splitToMeanTemperatureBranches(final KStream<String, Temperature> inputStream) {
        var result = splitToPredicatedBranches(inputStream);

        for (var TemperatureStream : result) {
            TemperatureStream.
                    groupByKey()
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(windowSize)))
                    .aggregate(
                            () -> TemperatureAggregation.newBuilder()
                                    .setData(Temperature.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel("")
                                            .setData(TemperatureDataRecord.newBuilder()
                                                    .setTemperature(INITIAL_VALUE)
                                                    .build())
                                            .build()
                                    )
                                    .setCount(0)
                                    .build(),
                            (key, value, aggregated) -> TemperatureAggregation.newBuilder()
                                    .setData(Temperature.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel(value.getLabel())
                                            .setData(TemperatureDataRecord.newBuilder()
                                                    .setTemperature(value.getData().getTemperature() + aggregated.getData().getData().getTemperature())
                                                    .build())
                                            .build())
                                    .setCount(aggregated.getCount() + 1)
                                    .build(),
                            Materialized.with(Serdes.String(), temperatureAggregationSerde)
                    )
                    .toStream()
                    .map((key, value) -> KeyValue.pair(key.key(),
                            Temperature.newBuilder()
                                    .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                    .setLabel(value.getData().getLabel())
                                    .setData(TemperatureDataRecord.newBuilder()
                                            .setTemperature(value.getData().getData().getTemperature() / value.getCount())
                                            .build())
                                    .build())
                    )
                    .to((key, value, recordContext) -> value.getLabel().toString() + outputTopicsPostfix,
                            Produced.with(Serdes.String(), temperatureSerde));

            if (debugEnabled) {
                TemperatureStream.print(Printed.toSysOut());
            }
        }

        return result;
    }

    public List<KStream<String, Temperature>> splitToMeanTemperatureBranches(final StreamsBuilder streamsBuilder) {
        final KStream<String, Temperature> stream = streamsBuilder.stream(this.inputTopic);
        return splitToMeanTemperatureBranches(stream);
    }
}
