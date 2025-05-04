package com.factory.kafka.config.factory.events;

import com.factory.kafka.config.model.EventsStreamConfig;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.message.GasComposition;
import com.factory.message.GasCompositionAggregation;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GasCompositionEventsStreamFactory extends EventsStreamFactory<GasComposition> {

    @Builder
    public GasCompositionEventsStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                           final EventsStreamConfig eventsStreamConfig) {
        super(kafkaNativeConfig, eventsStreamConfig);
    }

    @Override
    protected String getSensorId(GasComposition data) {
        return data.getLabel().toString();
    }

    @Override
    protected String getMetricName() {
        return "gasComposition";
    }

    @Override
    protected double getMetricValue(GasComposition data) {
        // Using the O2 level for threshold monitoring as an example
        // Could be adjusted to check multiple gas components or the most critical one
        return data.getData().getO2();
    }
}