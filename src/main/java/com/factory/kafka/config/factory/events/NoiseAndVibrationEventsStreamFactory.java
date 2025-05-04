package com.factory.kafka.config.factory.events;

import com.factory.kafka.config.model.EventsStreamConfig;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.message.NoiseAndVibration;
import com.factory.message.NoiseAndVibrationAggregation;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoiseAndVibrationEventsStreamFactory extends EventsStreamFactory<NoiseAndVibration> {

    @Builder
    public NoiseAndVibrationEventsStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                              final EventsStreamConfig eventsStreamConfig) {
        super(kafkaNativeConfig, eventsStreamConfig);
    }

    @Override
    protected String getSensorId(NoiseAndVibration data) {
        return data.getLabel().toString();
    }

    @Override
    protected String getMetricName() {
        return "vibration";
    }

    @Override
    protected double getMetricValue(NoiseAndVibration data) {
        // Using the vibration amplitude as the primary metric to check
        return data.getVibrationData().getAmplitude();
    }
    
    @Override
    protected String getWarningMessage(String metricName, double value, String sensorId) {
        return String.format("WARNING: Vibration amplitude %s exceeds warning threshold for %s",
                value, sensorId);
    }
    
    @Override
    protected String getCriticalMessage(String metricName, double value, String sensorId) {
        return String.format("CRITICAL: Vibration amplitude %s exceeds critical threshold for %s",
                value, sensorId);
    }
}