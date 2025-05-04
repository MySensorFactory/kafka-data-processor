package com.factory.kafka.config.factory.events;

import com.factory.kafka.config.model.EventsStreamConfig;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.message.FlowRate;
import com.factory.message.FlowRateAggregation;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlowRateEventsStreamFactory extends EventsStreamFactory<FlowRate> {

    @Builder
    public FlowRateEventsStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                      final EventsStreamConfig eventsStreamConfig) {
        super(kafkaNativeConfig, eventsStreamConfig);
    }

    @Override
    protected String getSensorId(FlowRate data) {
        return data.getLabel().toString();
    }

    @Override
    protected String getMetricName() {
        return "flowRate";
    }

    @Override
    protected double getMetricValue(FlowRate data) {
        return data.getData().getFlowRate();
    }
}