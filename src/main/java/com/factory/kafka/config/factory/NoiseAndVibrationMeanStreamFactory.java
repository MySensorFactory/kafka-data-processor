package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.MeanStreamConfig;
import com.factory.message.NoiseAndVibration;
import com.factory.message.NoiseAndVibrationAggregation;
import com.factory.message.NoiseDataRecord;
import com.factory.message.VibrationDataRecord;
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

public class NoiseAndVibrationMeanStreamFactory extends MeanStreamFactory<NoiseAndVibration, NoiseAndVibrationAggregation> {

    @Builder
    public NoiseAndVibrationMeanStreamFactory(final KafkaNativeConfig kafkaNativeConfig, final MeanStreamConfig config) {
        super(kafkaNativeConfig, config);
    }

    @Override
    protected Predicate[] preparePredicates() {
        return getLabels().stream()
                .map(label -> (Predicate<String, NoiseAndVibration>) (key, value) -> value.getLabel().toString().equals(label))
                .toArray(Predicate[]::new);
    }

    public List<KStream<String, NoiseAndVibration>> splitToMeanBranches(final KStream<String, NoiseAndVibration> inputStream) {
        var result = splitToPredicatedBranches(inputStream);

        for (var NoiseAndVibrationStream : result) {
            NoiseAndVibrationStream.
                    groupByKey()
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(getWindowSize())))
                    .aggregate(
                            () -> NoiseAndVibrationAggregation.newBuilder()
                                    .setData(NoiseAndVibration.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel("")
                                            .setVibrationData(VibrationDataRecord.newBuilder()
                                                    .setFrequency(INITIAL_VALUE)
                                                    .setAmplitude(INITIAL_VALUE)
                                                    .build())
                                            .setNoiseData(NoiseDataRecord.newBuilder()
                                                    .setLevel(INITIAL_VALUE)
                                                    .build())
                                            .build()
                                    )
                                    .setCount(0)
                                    .build(),
                            (key, value, aggregated) -> NoiseAndVibrationAggregation.newBuilder()
                                    .setData(NoiseAndVibration.newBuilder()
                                            .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                            .setLabel(value.getLabel())
                                            .setVibrationData(VibrationDataRecord.newBuilder()
                                                    .setFrequency(aggregated.getData().getVibrationData().getFrequency() + value.getVibrationData().getFrequency())
                                                    .setAmplitude(aggregated.getData().getVibrationData().getAmplitude() + value.getVibrationData().getAmplitude())
                                                    .build())
                                            .setNoiseData(NoiseDataRecord.newBuilder()
                                                    .setLevel(aggregated.getData().getNoiseData().getLevel() + value.getNoiseData().getLevel())
                                                    .build())
                                            .build())
                                    .setCount(aggregated.getCount() + 1)
                                    .build(),
                            Materialized.with(Serdes.String(), getAggregateSerdes())
                    )
                    .toStream()
                    .map((key, value) -> KeyValue.pair(key.key(),
                            NoiseAndVibration.newBuilder()
                                    .setTimestamp(ZonedDateTime.now().toEpochSecond())
                                    .setLabel(value.getData().getLabel())
                                    .setVibrationData(VibrationDataRecord.newBuilder()
                                            .setFrequency(value.getData().getVibrationData().getFrequency() / value.getCount())
                                            .setAmplitude(value.getData().getVibrationData().getAmplitude() / value.getCount())
                                            .build())
                                    .setNoiseData(NoiseDataRecord.newBuilder()
                                            .setLevel(value.getData().getNoiseData().getLevel() / value.getCount())
                                            .build())
                                    .build())
                    )
                    .to((key, value, recordContext) -> value.getLabel().toString() + getOutputTopicsPostfix(),
                            Produced.with(Serdes.String(), getSerde()));

            if (isDebugEnabled()) {
                NoiseAndVibrationStream.print(Printed.toSysOut());
            }
        }

        return result;
    }
}
