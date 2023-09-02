package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.StreamConfig;
import com.factory.message.FlowRate;
import com.factory.message.FlowRateAggregation;
import com.factory.message.FlowRateDataRecord;
import lombok.Builder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

public class FlowRateMeanStreamFactory extends MeanStreamFactory<FlowRate, FlowRateAggregation> {

    @Builder
    public FlowRateMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig, final StreamConfig config) {
        super(kafkaNativeConfig, config);
    }

    @Override
    protected Predicate[] preparePredicates() {
        return getLabels().stream()
                .map(label -> (Predicate<String, FlowRate>) (key, value) -> value.getLabel().toString().equals(label))
                .toArray(Predicate[]::new);
    }

    public List<KStream<String, FlowRate>> splitToMeanBranches(final KStream<String, FlowRate> inputStream) {
        var result = splitToPredicatedBranches(inputStream);

        for (var FlowRateStream : result) {
            FlowRateStream.
                    groupByKey()
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(getWindowSize())))
                    .aggregate(
                            () -> FlowRateAggregation.newBuilder()
                                    .setData(FlowRate.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel("")
                                            .setData(FlowRateDataRecord.newBuilder()
                                                    .setFlowRate(INITIAL_VALUE)
                                                    .build())
                                            .build()
                                    )
                                    .setCount(0)
                                    .build(),
                            (key, value, aggregated) -> FlowRateAggregation.newBuilder()
                                    .setData(FlowRate.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel(value.getLabel())
                                            .setData(FlowRateDataRecord.newBuilder()
                                                    .setFlowRate(value.getData().getFlowRate() + aggregated.getData().getData().getFlowRate())
                                                    .build())
                                            .build())
                                    .setCount(aggregated.getCount() + 1)
                                    .build(),
                            Materialized.with(Serdes.String(), getAggregateSerdes())
                    )
                    .toStream()
                    .map((key, value) -> KeyValue.pair(key.key(),
                            FlowRate.newBuilder()
                                    .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                    .setLabel(value.getData().getLabel())
                                    .setData(FlowRateDataRecord.newBuilder()
                                            .setFlowRate(value.getData().getData().getFlowRate() / value.getCount())
                                            .build())
                                    .build())
                    )
                    .to((key, value, recordContext) -> value.getLabel().toString() + getOutputTopicsPostfix(),
                            Produced.with(Serdes.String(), getSerde()));

            if (isDebugEnabled()) {
                FlowRateStream.print(Printed.toSysOut());
            }
        }

        return result;
    }
}
