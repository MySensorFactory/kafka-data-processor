package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.kafka.config.model.PerformanceStreamConfig;
import com.factory.message.Performance;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.time.ZonedDateTime;

public abstract class PerformanceStreamFactory<T> extends AbstractStreamFactory<Performance>{
    private final PerformanceStreamConfig performanceStreamConfig;

    protected PerformanceStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                       final PerformanceStreamConfig performanceStreamConfig) {
        super(kafkaNativeConfig);
        this.performanceStreamConfig = performanceStreamConfig;
    }

    public KStream<String, Performance> createPerformanceStream(final StreamsBuilder builder) {
        final int joinWindowsDurationMillis = performanceStreamConfig.getJoinWindowsDurationMillis();
        final String inputTopic  = performanceStreamConfig.getInputTopic();
        final String outputTopic = performanceStreamConfig.getOutputTopic();
        final String resultTopic = performanceStreamConfig.getResultTopic();

        KStream<String, T> inputStream = builder.stream(inputTopic);
        KStream<String, T> outputStream = builder.stream(outputTopic);

        var resultStream = inputStream.join(outputStream,
                (input, output) -> Performance.newBuilder()
                        .setTimestamp(ZonedDateTime.now().toEpochSecond())
                        .setValue(calculatePerformance(input, output))
                        .build(),
                JoinWindows.of(Duration.ofMillis(joinWindowsDurationMillis))
        );
        resultStream.to(resultTopic, Produced.with(Serdes.String(), getSerde()));

//        if(Boolean.TRUE.equals(performanceStreamConfig.getDebugEnabled())){
//            resultStream.print(Printed.toSysOut());
//        }

        return resultStream;
    }

    protected abstract float calculatePerformance(final T input, final T output);
}