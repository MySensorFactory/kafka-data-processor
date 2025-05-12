package com.factory.config

import com.typesafe.config.ConfigFactory
import pureconfig._
import pureconfig.generic.auto._

case class AppConfig(
  spark: SparkConfig,
  kafka: KafkaConfig,
  mean: MeanConfig,
  events: EventsConfig
)

case class SparkConfig(
  checkpointLocation: String,
  master: Option[String] = None,
  batchDuration: Long = 5000
)

case class KafkaConfig(
  bootstrapServers: String,
  schemaRegistryUrl: String
)

case class MeanConfig(
  enabled: Boolean,
  pressure: SensorConfig,
  temperature: SensorConfig,
  humidity: SensorConfig,
  vibration: SensorConfig
)

case class SensorConfig(
  enabled: Boolean,
  windowSize: Int,
  inputTopic: String,
  outputTopicsPostfix: String,
  labels: List[String],
  debugEnabled: Boolean
)

case class EventsConfig(
  enabled: Boolean,
  pressure: EventStreamConfig,
  temperature: EventStreamConfig,
  humidity: EventStreamConfig,
  vibration: EventStreamConfig,
  resultTopic: String,
  cooldownPeriodMs: Option[Long] = Some(5 * 60 * 1000) // Default 5 minutes
)

case class EventStreamConfig(
  enabled: Boolean,
  inputTopic: String,
  threshold: Double,
  criticalThreshold: Option[Double] = None, // If not set, will be calculated as 1.5 * threshold
  warningThreshold: Option[Double] = None,  // If not set, will use threshold value
  cooldownPeriodMs: Option[Long] = None,    // If not set, will use the global setting
  debugEnabled: Boolean
)

object AppConfig {
  def load(): AppConfig = {
    ConfigSource.default.load[AppConfig] match {
      case Right(config) => config
      case Left(failures) => 
        throw new RuntimeException(s"Failed to load configuration: ${failures.toList.mkString(", ")}")
    }
  }
} 