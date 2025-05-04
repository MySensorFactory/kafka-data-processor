package com.factory.kafka.config.factory.events;

import com.factory.kafka.config.factory.AbstractStreamFactory;
import com.factory.kafka.config.model.EventsStreamConfig;
import com.factory.kafka.config.model.KafkaNativeConfig;
import com.factory.message.Event;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Date;
import java.util.Map;

@Slf4j
@Getter
public abstract class EventsStreamFactory<T extends SpecificRecordBase> extends AbstractStreamFactory<T> {

    private final EventsStreamConfig eventsStreamConfig;
    private final String inputTopic;
    private final String outputTopic;
    private final Boolean debugEnabled;
    private final Map<String, EventsStreamConfig.SensorThresholds> thresholds;

    protected EventsStreamFactory(final KafkaNativeConfig kafkaNativeConfig,
                                  final EventsStreamConfig eventsStreamConfig) {
        super(kafkaNativeConfig);
        this.eventsStreamConfig = eventsStreamConfig;
        this.inputTopic = eventsStreamConfig.getInputTopic();
        this.outputTopic = eventsStreamConfig.getOutputTopic();
        this.debugEnabled = eventsStreamConfig.getDebugEnabled();
        this.thresholds = eventsStreamConfig.getThresholds();
    }

    public KStream<String, Event> createEventsStream(final StreamsBuilder builder) {
        KStream<String, T> inputStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), getSerde())
        );

        KStream<String, Event> eventsStream = inputStream
                .filter((key, value) -> value != null)
                .mapValues(this::analyzeData)
                .filter((key, event) -> event != null);

        eventsStream.to(
                outputTopic,
                Produced.with(Serdes.String(), prepareSerde())
        );

        if (Boolean.TRUE.equals(debugEnabled)) {
            eventsStream.peek((key, value) ->
                    log.info("Event produced: key={}, value={}", key, value));
        }

        return eventsStream;
    }

    // Store the last time an event was triggered for each sensor+severity
    private final Map<String, Long> lastEventTimeMap = new java.util.concurrent.ConcurrentHashMap<>();
    
    // Default cooldown period in milliseconds (5 minutes)
    private static final long DEFAULT_COOLDOWN_PERIOD_MS = 5 * 60 * 1000;
    
    protected Event analyzeData(T data) {
        String sensorId = getSensorId(data);
        String metricName = getMetricName();
        double value = getMetricValue(data);

        // Get current time for deduplication
        long currentTimeMs = System.currentTimeMillis();

        EventsStreamConfig.SensorThresholds sensorThresholds = thresholds.get(sensorId);
        if (sensorThresholds == null) {
            log.warn("No thresholds defined for sensor: {}", sensorId);
            return null;
        }

        Double criticalThreshold = sensorThresholds.getCriticalThreshold();
        Double warningThreshold = sensorThresholds.getWarningThreshold();
        
        // Check critical threshold first (higher priority)
        if (exceedsThreshold(value, criticalThreshold)) {
            String dedupeKey = sensorId + ":CRITICAL";
            
            // Check if we're in cooldown period
            if (shouldSuppressEvent(dedupeKey, currentTimeMs)) {
                log.debug("Suppressing duplicate critical event for sensor: {}", sensorId);
                return null;
            }
            
            // Record this event time
            lastEventTimeMap.put(dedupeKey, currentTimeMs);
            
            return Event.newBuilder()
                    .setTimestamp(getCurrentMillis())
                    .setTitle(getCriticalMessage(metricName, value, sensorId))
                    .setIsAlert(true)
                    .build();
        } 
        // Then check warning threshold
        else if (exceedsThreshold(value, warningThreshold)) {
            String dedupeKey = sensorId + ":WARNING";
            
            // Check if we're in cooldown period
            if (shouldSuppressEvent(dedupeKey, currentTimeMs)) {
                log.debug("Suppressing duplicate warning event for sensor: {}", sensorId);
                return null;
            }
            
            // Record this event time
            lastEventTimeMap.put(dedupeKey, currentTimeMs);
            
            return Event.newBuilder()
                    .setTimestamp(getCurrentMillis())
                    .setTitle(getWarningMessage(metricName, value, sensorId))
                    .setIsAlert(false)
                    .build();
        }
        // Value is back to normal - clear the cached event times to allow immediate alerts if it exceeds again
        else {
            String criticalKey = sensorId + ":CRITICAL";
            String warningKey = sensorId + ":WARNING";
            lastEventTimeMap.remove(criticalKey);
            lastEventTimeMap.remove(warningKey);
        }

        return null;
    }

    /**
     * Determines if an event should be suppressed based on cooldown period
     * @param key Unique key for sensor+severity 
     * @param currentTimeMs Current time in milliseconds
     * @return true if event should be suppressed, false otherwise
     */
    private boolean shouldSuppressEvent(String key, long currentTimeMs) {
        Long lastEventTime = lastEventTimeMap.get(key);
        if (lastEventTime == null) {
            return false; // No previous event, don't suppress
        }
        
        // Get cooldown period from config or use default
        Long cooldownPeriod = eventsStreamConfig.getCooldownPeriodMs();
        long cooldownMs = cooldownPeriod != null ? cooldownPeriod : DEFAULT_COOLDOWN_PERIOD_MS;
        
        // Check if we're still in cooldown period
        return (currentTimeMs - lastEventTime) < cooldownMs;
    }
    
    private static long getCurrentMillis() {
        return new Date().toInstant().getEpochSecond() * 1000;
    }

    protected abstract String getSensorId(T data);

    protected abstract String getMetricName();

    protected abstract double getMetricValue(T data);

    protected boolean exceedsThreshold(double value, Double threshold) {
        return threshold != null && value > threshold;
    }

    protected String getWarningMessage(String metricName, double value, String sensorId) {
        return String.format("WARNING: %s value %s exceeds warning threshold for sensor %s",
                metricName, value, sensorId);
    }

    protected String getCriticalMessage(String metricName, double value, String sensorId) {
        return String.format("CRITICAL: %s value %s exceeds critical threshold for sensor %s",
                metricName, value, sensorId);
    }
}