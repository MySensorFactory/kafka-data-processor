package com.factory.kafka.config.factory.mean;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.MeanStreamConfig;
import com.factory.message.Pressure;
import com.factory.message.PressureAggregation;
import com.factory.message.PressureDataRecord;
import lombok.Builder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.time.ZonedDateTime;
import java.util.List;

public class PressureMeanStreamFactory extends MeanStreamFactory<Pressure, PressureAggregation> {

    @Builder
    public PressureMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig, final MeanStreamConfig config) {
        super(kafkaNativeConfig, config);
    }

    @Override
    protected Predicate[] preparePredicates() {
        return getLabels().stream()
                .map(label -> (Predicate<String, Pressure>) (key, value) -> value.getLabel().toString().equals(label))
                .toArray(Predicate[]::new);
    }

    public List<KStream<String, Pressure>> splitToMeanBranches(final KStream<String, Pressure> inputStream) {
        var result = splitToPredicatedBranches(inputStream);

        for (var pressureStream : result) {
            pressureStream
                    .groupBy((key, value) -> value.getLabel().toString(),
                            Grouped.with(Serdes.String(), getSerde()))
                    .windowedBy(getWindowing())
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
                            Materialized.with(Serdes.String(), getAggregateSerdes())
                    )
                    .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
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
                    .to((key, value, recordContext) -> value.getLabel().toString() + getOutputTopicsPostfix(),
                            Produced.with(Serdes.String(), getSerde()));

//            if (isDebugEnabled()) {
//                pressureStream.print(Printed.toSysOut());
//            }
        }

        return result;
    }
}
