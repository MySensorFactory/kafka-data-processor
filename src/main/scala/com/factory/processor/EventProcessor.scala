package com.factory.processor

import com.factory.config.{EventStreamConfig, KafkaConfig}
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

case class SensorReading(
  label: String,
  timestampMs: Long,
  value: Double
)

case class ThresholdCooldownState(
  lastWarningSentMs: Option[Long],
  lastCriticalSentMs: Option[Long]
)

case class Event(
  title: String,
  timestampDaysEpoch: Long,
  is_alert: Boolean
)

class EventProcessor(
  spark: SparkSession,
  config: EventStreamConfig,
  kafkaConfig: KafkaConfig,
  sensorType: String,
  resultTopic: String
) extends Serializable {
  @transient lazy private val logger = LoggerFactory.getLogger(getClass)
  import spark.implicits._

  @transient lazy private val restService = new RestService(kafkaConfig.schemaRegistryUrl)

  private def addSchemaRegistryHeaderUDF(schemaId: Int) = udf((avroBytes: Array[Byte]) => {
    val result = new Array[Byte](1 + 4 + avroBytes.length)
    result(0) = 0 // Magic byte
    val schemaIdBytes = ByteBuffer.allocate(4).putInt(schemaId).array()
    System.arraycopy(schemaIdBytes, 0, result, 1, 4)
    System.arraycopy(avroBytes, 0, result, 5, avroBytes.length)
    result
  })

  private def processSensorReadings(
    label: String,
    readings: Iterator[SensorReading],
    state: GroupState[ThresholdCooldownState]
  ): Iterator[Event] = {
    if (state.hasTimedOut) {
      state.remove()
      Iterator.empty
    } else {
      var currentState = state.getOption.getOrElse(ThresholdCooldownState(None, None))
      var eventsToEmit = List.empty[Event]

      readings.toList.sortBy(_.timestampMs).foreach { reading =>
        val currentTimeMs = reading.timestampMs
        val value = reading.value

        // Check critical threshold first
        if (config.criticalThreshold.exists(value > _)) {
          val timeSinceLastCritical = currentState.lastCriticalSentMs.map(currentTimeMs - _)
          val cooldownExpired = (timeSinceLastCritical, config.cooldownPeriodMs) match {
            case (None, _) => true // No previous event, so cooldown is expired
            case (Some(diff), Some(cooldown)) => diff >= cooldown
            case (Some(_), None) => true // No cooldown configured, so always expired
            case (None, Some(_)) => true // No previous event, so cooldown is expired
          }
          
          if (cooldownExpired) {
            eventsToEmit :+= Event(
              title = s"CRITICAL: $sensorType value $value exceeds critical threshold for sensor $label",
              timestampDaysEpoch = TimeUnit.MILLISECONDS.toDays(currentTimeMs),
              is_alert = true
            )
            currentState = currentState.copy(lastCriticalSentMs = Some(currentTimeMs))
          }
        }
        // Then check warning threshold
        else if (config.warningThreshold.exists(value > _)) {
          val timeSinceLastWarning = currentState.lastWarningSentMs.map(currentTimeMs - _)
          val cooldownExpired = (timeSinceLastWarning, config.cooldownPeriodMs) match {
            case (None, _) => true // No previous event, so cooldown is expired
            case (Some(diff), Some(cooldown)) => diff >= cooldown
            case (Some(_), None) => true // No cooldown configured, so always expired
            case (None, Some(_)) => true // No previous event, so cooldown is expired
          }
          
          if (cooldownExpired) {
            eventsToEmit :+= Event(
              title = s"WARNING: $sensorType value $value exceeds warning threshold for sensor $label",
              timestampDaysEpoch = TimeUnit.MILLISECONDS.toDays(currentTimeMs),
              is_alert = false
            )
            currentState = currentState.copy(lastWarningSentMs = Some(currentTimeMs))
          }
        }
        // Value is back to normal - reset cooldown state
        else {
          currentState = ThresholdCooldownState(None, None)
        }
      }

      if (eventsToEmit.nonEmpty || currentState != state.getOption.orNull) {
        state.update(currentState)
      }

      eventsToEmit.iterator
    }
  }

  def start(): Unit = {
    val eventOutputSubjectName = "events-value"
    val eventAvroSchemaForFunc = restService.getLatestVersion(eventOutputSubjectName).getSchema
    val eventSchemaId = restService.getLatestVersion(eventOutputSubjectName).getId

    val subjectPostfix = sensorType.substring(0, 1).toUpperCase + sensorType.substring(1)
    logger.warn(s"subjectPostfix $subjectPostfix")
    val inputAvroSchemaString = restService.getLatestVersion("com.factory.message." + subjectPostfix).getSchema

    val inputStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", config.inputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val deserializedStream: Dataset[SensorReading] = inputStream
      .select(from_avro(expr("substring(value, 6)"), inputAvroSchemaString).as("avro_data"))
      .select(
        col("avro_data.label").as("label"),
        from_unixtime(col("avro_data.timestamp")).cast("timestamp").as("timestampMs"),
        col(s"avro_data.data.$sensorType").as("value")
      )
      .as[SensorReading]

    val eventStream = deserializedStream
      .withWatermark("timestampMs", "1 minute")
      .groupByKey(_.label)
      .flatMapGroupsWithState[ThresholdCooldownState, Event](
        OutputMode.Append(),
        GroupStateTimeout.ProcessingTimeTimeout
      )(processSensorReadings)

    val finalEventStream = eventStream
      .select(
        col("title").as("key"),
        addSchemaRegistryHeaderUDF(eventSchemaId)(
          to_avro(
            struct(
              col("title"),
              col("timestampDaysEpoch").as("timestamp"),
              col("is_alert")
            ),
            eventAvroSchemaForFunc
          )
        ).as("value")
      )

    val checkpointBaseDir = spark.conf.get("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints_eventproc")
    val checkpointLocation = s"$checkpointBaseDir/events_processor_v4/$sensorType"
    logger.info(s"Using checkpoint location: $checkpointLocation")

    finalEventStream
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("topic", resultTopic)
      .option("checkpointLocation", checkpointLocation)
      .outputMode("append")
      .start()

    if (config.debugEnabled) {
      eventStream
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", false)
        .start()
    }
  }
}