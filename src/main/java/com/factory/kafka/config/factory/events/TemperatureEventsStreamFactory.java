package com.factory.kafka.config.factory.events;

import com.factory.kafka.config.model.EventsStreamConfig;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.message.Temperature;
import com.factory.message.TemperatureAggregation;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TemperatureEventsStreamFactory extends EventsStreamFactory<Temperature> {

    @Builder
    public TemperatureEventsStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                         final EventsStreamConfig eventsStreamConfig) {
        super(kafkaNativeConfig, eventsStreamConfig);
    }


    @Override
    protected String getSensorId(Temperature data) {
        return data.getLabel().toString();
    }

    @Override
    protected String getMetricName() {
        return "temperature";
    }

    @Override
    protected double getMetricValue(Temperature data) {
        return data.getData().getTemperature();
    }
}