package com.factory.kafka.config.factory.mean;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.MeanStreamConfig;
import com.factory.message.GasComposition;
import com.factory.message.GasCompositionAggregation;
import com.factory.message.GasCompositionDataRecord;
import lombok.Builder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.time.ZonedDateTime;
import java.util.List;

public class GasCompositionMeanStreamFactory extends MeanStreamFactory<GasComposition, GasCompositionAggregation> {

    @Builder
    public GasCompositionMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig, final MeanStreamConfig config) {
        super(kafkaNativeConfig, config);
    }

    @Override
    protected Predicate[] preparePredicates() {
        return getLabels().stream()
                .map(label -> (Predicate<String, GasComposition>) (key, value) -> value.getLabel().toString().equals(label))
                .toArray(Predicate[]::new);
    }

    public List<KStream<String, GasComposition>> splitToMeanBranches(final KStream<String, GasComposition> inputStream) {
        var result = splitToPredicatedBranches(inputStream);

        for (var GasCompositionStream : result) {
            GasCompositionStream
                    .groupBy((key, value) -> value.getLabel().toString(),
                            Grouped.with(Serdes.String(), getSerde()))
                    .windowedBy(getWindowing())
                    .aggregate(
                            () -> GasCompositionAggregation.newBuilder()
                                    .setData(GasComposition.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel("")
                                            .setData(GasCompositionDataRecord.newBuilder()
                                                    .setH2(INITIAL_VALUE)
                                                    .setCo2(INITIAL_VALUE)
                                                    .setN2(INITIAL_VALUE)
                                                    .setNh3(INITIAL_VALUE)
                                                    .setO2(INITIAL_VALUE)
                                                    .build())
                                            .build()
                                    )
                                    .setCount(0)
                                    .build(),
                            (key, value, aggregated) -> GasCompositionAggregation.newBuilder()
                                    .setData(GasComposition.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel(value.getLabel())
                                            .setData(GasCompositionDataRecord.newBuilder()
                                                    .setH2(aggregated.getData().getData().getH2() + value.getData().getH2())
                                                    .setCo2(aggregated.getData().getData().getCo2() + value.getData().getCo2())
                                                    .setN2(aggregated.getData().getData().getN2() + value.getData().getN2())
                                                    .setNh3(aggregated.getData().getData().getNh3() + value.getData().getNh3())
                                                    .setO2(aggregated.getData().getData().getO2() + value.getData().getO2())
                                                    .build())
                                            .build())
                                    .setCount(aggregated.getCount() + 1)
                                    .build(),
                            Materialized.with(Serdes.String(), getAggregateSerdes())
                    )
                    .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                    .toStream()
                    .map((key, value) -> KeyValue.pair(key.key(),
                            GasComposition.newBuilder()
                                    .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                    .setLabel(value.getData().getLabel())
                                    .setData(GasCompositionDataRecord.newBuilder()
                                            .setH2(value.getData().getData().getH2() / value.getCount())
                                            .setCo2(value.getData().getData().getCo2() / value.getCount())
                                            .setN2(value.getData().getData().getN2() / value.getCount())
                                            .setNh3(value.getData().getData().getNh3() / value.getCount())
                                            .setO2(value.getData().getData().getO2() / value.getCount())
                                            .build())
                                    .build())
                    )
                    .to((key, value, recordContext) -> value.getLabel().toString() + getOutputTopicsPostfix(),
                            Produced.with(Serdes.String(), getSerde()));

//            if (isDebugEnabled()) {
//                GasCompositionStream.print(Printed.toSysOut());
//            }
        }

        return result;
    }
}
