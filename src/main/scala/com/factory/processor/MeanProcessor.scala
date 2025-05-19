package com.factory.processor

import com.factory.config.{KafkaConfig, SensorConfig}
import com.factory.model.SensorReading
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, SparkSession}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer


class MeanProcessor[T <: SensorReading : Encoder](
                                                   spark: SparkSession,
                                                   config: SensorConfig,
                                                   kafkaConfig: KafkaConfig,
                                                   sensorType: String
                                                 ) {
  private val logger = LoggerFactory.getLogger(getClass)


  private def addSchemaRegistryHeaderUDF(schemaId: Int) = udf((avroBytes: Array[Byte]) => {
    val result = new Array[Byte](1 + 4 + avroBytes.length)

    result(0) = 0

    val schemaIdBytes = ByteBuffer.allocate(4).putInt(schemaId).array()
    System.arraycopy(schemaIdBytes, 0, result, 1, 4)

    System.arraycopy(avroBytes, 0, result, 5, avroBytes.length)

    result
  })

  def start(): Unit = {
    import spark.implicits._

    logger.info(s"Starting mean calculation for $sensorType")
    val restService = new RestService(kafkaConfig.schemaRegistryUrl)

    val inputStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", config.inputTopic)
      .option("startingOffsets", "latest")
      .load()

    config.labels.foreach { label =>
      logger.warn(s"Setting up processing for $sensorType label: $label")

      val outputSchema = restService.getLatestVersion(sensorType + "Mean" + "-value")
      val subjectPostfix = sensorType.substring(0, 1).toUpperCase + sensorType.substring(1)
      logger.warn(s"subjectPostfix $subjectPostfix")
      val jsonFormatSchema = restService.getLatestVersion("com.factory.message." + subjectPostfix)

      val valueDF = inputStream
        .withColumn("stripped_value", expr("substring(value, 6)")) //see last comment in: https://stackoverflow.com/questions/59222774/spark-from-avro-function-returning-null-values
        .select(from_avro(data = $"stripped_value", jsonFormatSchema.getSchema).as(s"$sensorType"))
        .filter(col(s"$sensorType.label") === label)
        .withColumn("event_time", from_unixtime(col(s"$sensorType.timestamp")).cast("timestamp"))

      val aggregatedDS = valueDF
        .withWatermark("event_time", s"1 minutes")
        .groupBy(window(col("event_time"), s"2 minutes", s"1 minutes"), $"$sensorType.label")
        .agg(
          avg(col(s"$sensorType.data.$sensorType")).cast("float").as(s"$sensorType"),
          count("*").cast("int").as("count"),
        )
        .filter(row => !row.isNullAt(1))
        .withColumn("label", lit(label))
        .withColumn("timestamp", unix_timestamp(col("window.end")))
        .filter(col("count") > 0)
        .select(
          struct(
            struct(
              col("label"),
              col("timestamp"),
              struct(
                col(s"$sensorType")
              ).as("data")
            ).as("data"),
            col("count")
          ).as("result"),
          col("timestamp")
        )
        .select(
          to_avro($"result", outputSchema.getSchema).as("pre_value"),
          col("timestamp").cast("string").as("key")
        )
        .withColumn("value", addSchemaRegistryHeaderUDF(outputSchema.getId)(col("pre_value")))

      aggregatedDS
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
        .option("topic", s"${sensorType}Mean")
        .outputMode("append")
        .start()

      if (config.debugEnabled) {
        aggregatedDS
          .writeStream
          .format("console")
          .outputMode("append")
          .option("truncate", false)
          .start()
      }
    }
  }
} 