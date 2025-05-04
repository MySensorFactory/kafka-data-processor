package com.factory.kafka.config.factory.events;

import com.factory.kafka.config.model.EventsStreamConfig;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.message.Pressure;
import com.factory.message.PressureAggregation;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PressureEventsStreamFactory extends EventsStreamFactory<Pressure> {

    @Builder
    public PressureEventsStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                      final EventsStreamConfig eventsStreamConfig) {
        super(kafkaNativeConfig, eventsStreamConfig);
    }

    @Override
    protected String getSensorId(Pressure data) {
        return data.getLabel().toString();
    }

    @Override
    protected String getMetricName() {
        return "pressure";
    }

    @Override
    protected double getMetricValue(Pressure data) {
        return data.getData().getPressure();
    }
}