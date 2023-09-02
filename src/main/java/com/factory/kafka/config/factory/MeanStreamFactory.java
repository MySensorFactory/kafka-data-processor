package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.StreamConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Arrays;
import java.util.List;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;

@Getter
public abstract class MeanStreamFactory<RecordType extends SpecificRecordBase,
        RecordAggregationType extends SpecificRecordBase> {

    public static final float INITIAL_VALUE = 0;
    private final int windowSize;
    private final List<String> labels;
    private final boolean debugEnabled;
    private final String outputTopicsPostfix;
    private final String inputTopic;
    private final String schemaRegistryUrl;
    private final Serde<RecordType> serde;
    private final Serde<RecordAggregationType> aggregateSerdes;
    private final Predicate[] predicates;

    protected MeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                final StreamConfig config) {
        this.windowSize = config.getWindowSize();
        this.labels = config.getLabels();
        this.debugEnabled = config.getDebugEnabled();
        this.outputTopicsPostfix = config.getOutputTopicsPostfix();
        this.inputTopic = config.getInputTopic();
        this.schemaRegistryUrl = kafkaNativeConfig.getSchemaRegistryUrl();
        this.serde = prepareSerde();
        this.aggregateSerdes = prepareSerde();
        this.predicates = preparePredicates();
    }

    private <U extends SpecificRecordBase> Serde<U> prepareSerde() {
        final Serde<U> result = new SpecificAvroSerde<>();
        result.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return result;
    }

    protected abstract Predicate[] preparePredicates();

    public List<KStream<String, RecordType>> splitToPredicatedBranches(final KStream<String, RecordType> inputStream) {
        final KStream<String, RecordType>[] result = inputStream.branch(predicates);
        return Arrays.stream(result).toList();

    }

    public abstract List<KStream<String, RecordType>> splitToMeanBranches(final KStream<String, RecordType> inputStream);

    public List<KStream<String, RecordType>> splitToMeanBranches(final StreamsBuilder streamsBuilder) {
        final KStream<String, RecordType> stream = streamsBuilder.stream(this.inputTopic);
        return splitToMeanBranches(stream);
    }
}
