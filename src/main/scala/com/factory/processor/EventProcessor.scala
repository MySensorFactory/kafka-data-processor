package com.factory.processor

import com.factory.config.EventStreamConfig
import com.factory.model._
import com.factory.util.AvroUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

class EventProcessor(
  spark: SparkSession,
  config: EventStreamConfig,
  sensorType: String,
  resultTopic: String
) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Store the last time an event was triggered for each sensor+severity
  private val lastEventTimeMap = new ConcurrentHashMap[String, Long]()
  
  // Default cooldown period in milliseconds (5 minutes)
  private val DEFAULT_COOLDOWN_PERIOD_MS = 5 * 60 * 1000L
  
  def start(): Unit = {
    import spark.implicits._
    
    logger.info(s"Starting event detection for $sensorType")
    
    // Read input topic
    val inputStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", spark.conf.get("kafka.bootstrap.servers"))
      .option("subscribe", config.inputTopic)
      .option("startingOffsets", "latest")
      .load()
      
    // Get value to check based on sensor type
    val valueExtractorFunction = sensorType match {
      case "pressure" => (bytes: Array[Byte]) => {
        val sensor = AvroUtils.deserialize[Pressure](bytes, sensorType)
        (sensor.timestamp, sensor.label, sensor.data.pressure)
      }
      case "temperature" => (bytes: Array[Byte]) => {
        val sensor = AvroUtils.deserialize[Temperature](bytes, sensorType)
        (sensor.timestamp, sensor.label, sensor.data.temperature)
      }
      case "humidity" => (bytes: Array[Byte]) => {
        val sensor = AvroUtils.deserialize[Humidity](bytes, sensorType)
        (sensor.timestamp, sensor.label, sensor.data.humidity)
      }
      case "vibration" => (bytes: Array[Byte]) => {
        val sensor = AvroUtils.deserialize[Vibration](bytes, sensorType)
        // For simplicity, just monitor noise level
        (sensor.timestamp, sensor.label, sensor.data.vibration)
      }
      case _ => throw new IllegalArgumentException(s"Unsupported sensor type: $sensorType")
    }
    
    // Process and detect events
    val eventStream = inputStream
      .select("value")
      .as[Array[Byte]]
      .map { bytes =>
        val (timestamp, sensorId, value) = valueExtractorFunction(bytes)
        
        // Get thresholds from configuration
        val criticalThreshold = config.criticalThreshold.getOrElse(config.threshold * 1.5)
        val warningThreshold = config.warningThreshold.getOrElse(config.threshold)
        
        // Get current time for deduplication
        val currentTimeMs = System.currentTimeMillis()
        
        // Create the relevant metric name based on sensor type
        val metricName = sensorType match {
          case "pressure" => "Pressure"
          case "temperature" => "Temperature"
          case "flowRate" => "Flow Rate"
          case "noiseAndVibration" => "Noise Level"
          case _ => sensorType
        }
        
        // Check critical threshold first (higher priority)
        if (exceedsThreshold(value, criticalThreshold)) {
          val dedupeKey = s"$sensorId:CRITICAL"
          
          // Check if we're in cooldown period
          if (shouldSuppressEvent(dedupeKey, currentTimeMs, config.cooldownPeriodMs.getOrElse(DEFAULT_COOLDOWN_PERIOD_MS))) {
            logger.debug(s"Suppressing duplicate critical event for sensor: $sensorId")
            None
          } else {
            // Record this event time
            lastEventTimeMap.put(dedupeKey, currentTimeMs)
            
            Some(Event(
              timestamp,
              sensorType,
              sensorId,
              value,
              criticalThreshold,
              getCriticalMessage(metricName, value, sensorId),
              isAlert = true
            ))
          }
        } 
        // Then check warning threshold
        else if (exceedsThreshold(value, warningThreshold)) {
          val dedupeKey = s"$sensorId:WARNING"
          
          // Check if we're in cooldown period
          if (shouldSuppressEvent(dedupeKey, currentTimeMs, config.cooldownPeriodMs.getOrElse(DEFAULT_COOLDOWN_PERIOD_MS))) {
            logger.debug(s"Suppressing duplicate warning event for sensor: $sensorId")
            None
          } else {
            // Record this event time
            lastEventTimeMap.put(dedupeKey, currentTimeMs)
            
            Some(Event(
              timestamp,
              sensorType,
              sensorId,
              value,
              warningThreshold,
              getWarningMessage(metricName, value, sensorId),
              isAlert = false
            ))
          }
        }
        // Value is back to normal - clear the cached event times to allow immediate alerts if it exceeds again
        else {
          val criticalKey = s"$sensorId:CRITICAL"
          val warningKey = s"$sensorId:WARNING"
          lastEventTimeMap.remove(criticalKey)
          lastEventTimeMap.remove(warningKey)
          None
        }
      }
      .filter(_.isDefined)
      .map(_.get)
    
    // Write events to output topic
    val query = eventStream
      .select(
        expr("sensorType || '-' || sensorId").as("key"),
        AvroUtils.serializeEventToColumn.as("value")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", spark.conf.get("kafka.bootstrap.servers"))
      .option("topic", resultTopic)
      .option("checkpointLocation", s"${spark.conf.get("spark.sql.streaming.checkpointLocation")}/events/$sensorType")
      .trigger(Trigger.ProcessingTime(1000))
      .start()
      
    if (config.debugEnabled) {
      eventStream
        .writeStream
        .format("console")
        .option("truncate", false)
        .start()
    }
  }
  
  private def shouldSuppressEvent(key: String, currentTimeMs: Long, cooldownMs: Long): Boolean = {
    Option(lastEventTimeMap.get(key)) match {
      case Some(lastEventTime) => 
        (currentTimeMs - lastEventTime) < cooldownMs
      case None => 
        false // No previous event, don't suppress
    }
  }
  
  private def exceedsThreshold(value: Double, threshold: Double): Boolean = {
    value > threshold
  }
  
  private def getWarningMessage(metricName: String, value: Double, sensorId: String): String = {
    s"WARNING: $metricName value $value exceeds warning threshold for sensor $sensorId"
  }
  
  private def getCriticalMessage(metricName: String, value: Double, sensorId: String): String = {
    s"CRITICAL: $metricName value $value exceeds critical threshold for sensor $sensorId"
  }
} 