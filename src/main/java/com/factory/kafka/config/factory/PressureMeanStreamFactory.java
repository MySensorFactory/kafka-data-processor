package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.StreamConfig;
import com.factory.message.Pressure;
import com.factory.message.PressureAggregation;
import com.factory.message.PressureDataRecord;
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

public class PressureMeanStreamFactory {
    public static final float INITIAL_VALUE = 0;
    private final int windowSize;
    private final List<String> labels;
    private final boolean debugEnabled;
    private final String outputTopicsPostfix;
    private final String inputTopic;
    private final String schemaRegistryUrl;
    private final Serde<Pressure> pressureSerdes;
    private final Serde<PressureAggregation> pressureAggregateSerdes;
    private final Predicate[] predicates;

    @Builder
    public PressureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                     final StreamConfig config) {
        this.windowSize = config.getWindowSize();
        this.labels = config.getLabels();
        this.debugEnabled = config.getDebugEnabled();
        this.outputTopicsPostfix = config.getOutputTopicsPostfix();
        this.inputTopic = config.getInputTopic();
        this.schemaRegistryUrl = kafkaNativeConfig.getSchemaRegistryUrl();
        this.pressureSerdes = new SpecificAvroSerde<>();
        pressureSerdes.configure(
                singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                , false);
        this.pressureAggregateSerdes = new SpecificAvroSerde<>();
        pressureAggregateSerdes.configure(
                singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                , false);
        this.predicates = labels.stream()
                .map(label -> (Predicate<String, Pressure>) (key, value) -> value.getLabel().toString().equals(label))
                .toArray(Predicate[]::new);
    }

    public List<KStream<String, Pressure>> splitToPredicatedBranches(final KStream<String, Pressure> inputStream) {
        final KStream<String, Pressure>[] result = inputStream.branch(predicates);
        return Arrays.stream(result).toList();

    }

    public List<KStream<String, Pressure>> splitToMeanPressureBranches(final KStream<String, Pressure> inputStream) {
        var result = splitToPredicatedBranches(inputStream);

        for (var pressureStream : result) {
            pressureStream.
                    groupByKey()
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(windowSize)))
                    .aggregate(
                            () -> PressureAggregation.newBuilder()
                                    .setData(Pressure.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel("")
                                            .setData(PressureDataRecord.newBuilder()
                                                    .setPressure(INITIAL_VALUE)
                                                    .build())
                                            .build()
                                    )
                                    .setCount(0)
                                    .build(),
                            (key, value, aggregated) -> PressureAggregation.newBuilder()
                                    .setData(Pressure.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel(value.getLabel())
                                            .setData(PressureDataRecord.newBuilder()
                                                    .setPressure(value.getData().getPressure() + aggregated.getData().getData().getPressure())
                                                    .build())
                                            .build())
                                    .setCount(aggregated.getCount() + 1)
                                    .build(),
                            Materialized.with(Serdes.String(), pressureAggregateSerdes)
                    )
                    .toStream()
                    .map((key, value) -> KeyValue.pair(key.key(),
                            Pressure.newBuilder()
                                    .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                    .setLabel(value.getData().getLabel())
                                    .setData(PressureDataRecord.newBuilder()
                                            .setPressure(value.getData().getData().getPressure() / value.getCount())
                                            .build())
                                    .build())
                    )
                    .to((key, value, recordContext) -> value.getLabel().toString() + outputTopicsPostfix,
                            Produced.with(Serdes.String(), pressureSerdes));

            if (debugEnabled) {
                pressureStream.print(Printed.toSysOut());
            }
        }

        return result;
    }

    public List<KStream<String, Pressure>> splitToMeanPressureBranches(final StreamsBuilder streamsBuilder) {
        final KStream<String, Pressure> stream = streamsBuilder.stream(this.inputTopic);
        return splitToMeanPressureBranches(stream);
    }
}
