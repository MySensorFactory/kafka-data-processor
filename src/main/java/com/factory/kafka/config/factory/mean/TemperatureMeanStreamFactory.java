package com.factory.kafka.config.factory.mean;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.MeanStreamConfig;
import com.factory.message.Temperature;
import com.factory.message.TemperatureAggregation;
import com.factory.message.TemperatureDataRecord;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.time.ZonedDateTime;
import java.util.List;

@Slf4j
public class TemperatureMeanStreamFactory extends MeanStreamFactory<Temperature, TemperatureAggregation> {

    @Builder
    public TemperatureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig, final MeanStreamConfig config) {
        super(kafkaNativeConfig, config);
    }

    @Override
    protected Predicate[] preparePredicates() {
        return getLabels().stream()
                .map(label -> (Predicate<String, Temperature>) (key, value) -> value.getLabel().toString().equals(label))
                .toArray(Predicate[]::new);
    }

    public List<KStream<String, Temperature>> splitToMeanBranches(final KStream<String, Temperature> inputStream) {
        var result = splitToPredicatedBranches(inputStream);

        for (var TemperatureStream : result) {
            TemperatureStream
                    .groupBy((key, value) -> value.getLabel().toString(),
                            Grouped.with(Serdes.String(), getSerde()))
                    .windowedBy(getWindowing())
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
                            Materialized.with(Serdes.String(), getAggregateSerdes())
                    )
                    .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
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
                    .to((key, value, recordContext) -> value.getLabel().toString() + getOutputTopicsPostfix(),
                            Produced.with(Serdes.String(), getSerde()));

//            if (isDebugEnabled()) {
//                TemperatureStream.print(Printed.toSysOut());
//            }
        }

        return result;
    }

}
