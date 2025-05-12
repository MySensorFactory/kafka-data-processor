package com.factory

import com.factory.config.AppConfig
import com.factory.model._
import com.factory.processor.MeanProcessor
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory

object SparkDataProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Data Processor")

    val config = AppConfig.load()

    val spark = SparkSession.builder()
      .appName("Spark Data Processor")
      .config("spark.sql.streaming.checkpointLocation", config.spark.checkpointLocation)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      if (config.mean.enabled) {
        logger.info("Initializing mean processors")
        if (config.mean.pressure.enabled) {
          new MeanProcessor[Pressure](spark, config.mean.pressure, config.kafka, "pressure")(Encoders.product[Pressure]).start()
        }
        if (config.mean.temperature.enabled) {
          new MeanProcessor[Temperature](spark, config.mean.temperature, config.kafka, "temperature")(Encoders.product[Temperature]).start()
        }
        if (config.mean.humidity.enabled) {
          new MeanProcessor[Humidity](spark, config.mean.humidity, config.kafka, "humidity")(Encoders.product[Humidity]).start()
        }
        if (config.mean.vibration.enabled) {
          new MeanProcessor[Vibration](spark, config.mean.vibration, config.kafka, "vibration")(Encoders.product[Vibration]).start()
        }
      }

      //      if (config.events.enabled) {
      //        logger.info("Initializing event processors")
      //        if (config.events.pressure.enabled) {
      //          new EventProcessor(spark, config.events.pressure, "pressure", config.events.resultTopic).start()
      //        }
      //        if (config.events.temperature.enabled) {
      //          new EventProcessor(spark, config.events.temperature, "temperature", config.events.resultTopic).start()
      //        }
      //        if (config.events.flowRate.enabled) {
      //          new EventProcessor(spark, config.events.flowRate, "flowRate", config.events.resultTopic).start()
      //        }
      //        if (config.events.noiseAndVibration.enabled) {
      //          new EventProcessor(spark, config.events.noiseAndVibration, "noiseAndVibration", config.events.resultTopic).start()
      //        }
      //      }

      spark.streams.awaitAnyTermination()
    } catch {
      case e: Exception =>
        logger.error(s"Error in Spark Data Processor: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }
} 