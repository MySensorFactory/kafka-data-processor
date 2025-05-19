package com.factory.processor

import com.factory.config.KafkaConfig
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.sql.Timestamp

class EquipmentStateProcessor(
                               spark: SparkSession,
                               kafkaConfig: KafkaConfig
                             ) {
  private val logger = LoggerFactory.getLogger(getClass)

  import spark.implicits._

  private lazy val restService = new RestService(kafkaConfig.schemaRegistryUrl)
  val model: PipelineModel = PipelineModel.load("/opt/spark/app-jars/equipment_state_spark_model")

  private val pressureOutputAvroSchemaJson = restService.getLatestVersion("pressureAugumented-value")
  private val temperatureOutputAvroSchemaJson = restService.getLatestVersion("temperatureAugumented-value")
  private val humidityOutputAvroSchemaJson = restService.getLatestVersion("humidityAugumented-value")
  private val vibrationOutputAvroSchemaJson = restService.getLatestVersion("vibrationAugumented-value")

  private def addSchemaRegistryHeaderUDF(schemaId: Int) = udf((avroBytes: Array[Byte]) => {
    val result = new Array[Byte](1 + 4 + avroBytes.length)
    result(0) = 0 // Magic byte
    val schemaIdBytes = ByteBuffer.allocate(4).putInt(schemaId).array()
    System.arraycopy(schemaIdBytes, 0, result, 1, 4)
    System.arraycopy(avroBytes, 0, result, 5, avroBytes.length)
    result
  })

  private def readAndPrepareSensorStream(
                                          topicName: String,
                                          inputSchemaSubjectBaseName: String, // e.g., "Pressure", "Temperature"
                                          valueFieldName: String
                                        ): Dataset[Row] = {
    val fullInputSubjectName = s"com.factory.message.$inputSchemaSubjectBaseName"
    logger.info(s"Fetching input Avro schema for subject: $fullInputSubjectName from topic: $topicName")
    val avroSchemaJson = restService.getLatestVersion(fullInputSubjectName).getSchema
    logger.debug(s"Input Avro schema for $fullInputSubjectName: $avroSchemaJson")

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .load()
      .select(
        col("key"),
        from_avro(expr("substring(value, 6)"), avroSchemaJson).as("avro_data")
      )
      .select(
        col("key"),
        col("avro_data.label").as("label"),
        col("avro_data.timestamp").cast("timestamp").as("eventTime"), // ms to Spark Timestamp
        col("avro_data.timestamp").as("timestamp_sec"),
        col(s"avro_data.data.$valueFieldName").cast("float").as(valueFieldName)
      )
  }

  def start(): Unit = {
    logger.info("Starting Equipment State Processor")

    val watermarkDuration = "3 minutes"

    val pressureEvents = readAndPrepareSensorStream("pressure", "Pressure", "pressure")
      .withWatermark("eventTime", watermarkDuration).alias("p")
    val temperatureEvents = readAndPrepareSensorStream("temperature", "Temperature", "temperature")
      .withWatermark("eventTime", watermarkDuration).alias("t")
    val humidityEvents = readAndPrepareSensorStream("humidity", "Humidity", "humidity")
      .withWatermark("eventTime", watermarkDuration).alias("h")
    val vibrationEvents = readAndPrepareSensorStream("vibration", "Vibration", "vibration")
      .withWatermark("eventTime", watermarkDuration).alias("v")

    val joinedData = pressureEvents
      .join(temperatureEvents, "key")
      .join(humidityEvents, "key")
      .join(vibrationEvents, "key")
      .select(
        col("p.label").as("equipment_type"),
        col("p.eventTime").as("event_timestamp"),
        col("p.pressure").as("pressure"),
        col("t.temperature").as("temperature"),
        col("h.humidity").as("humidity"),
        col("v.vibration").as("vibration")
      )

    val predictionsDF = model.transform(joinedData)

    val sensorTypes = Seq(
      ("pressure", pressureOutputAvroSchemaJson, "pressure"),
      ("temperature", temperatureOutputAvroSchemaJson, "temperature"),
      ("humidity", humidityOutputAvroSchemaJson, "humidity"),
      ("vibration", vibrationOutputAvroSchemaJson, "vibration")
    )

    sensorTypes.foreach { case (sensorName, outputSchemaJson, baseOutputName) =>

      val outputSubjectName = s"${baseOutputName}Augumented-value"
      val outputTopicName = s"${baseOutputName}Augumented"

      logger.info(s"Setting up output for $outputTopicName with subject $outputSubjectName")

      val schemaId = restService.getLatestVersion(outputSubjectName).getId

      val dfForAvro = predictionsDF.select(
        struct(
          col("equipment_type").as("label"),
          unix_timestamp(col("event_timestamp")).as("timestamp"),
          struct(col(sensorName).as(sensorName)).as("data"),
          col("predicted_status").as("predictedState")
        ).as("payload")
      )

      val finalOutputDf = dfForAvro
        .select(
          col("payload.label").as("key"),
          addSchemaRegistryHeaderUDF(schemaId)(to_avro(col("payload"), outputSchemaJson.getSchema)).as("value")
        )

      finalOutputDf.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
        .option("topic", outputTopicName)
        .start()

      finalOutputDf
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", false)
        .start()

      logger.info(s"Started Kafka output stream for $outputTopicName")
    }
    logger.info(s"All Equipment State Prediction output streams to Kafka started.")
  }
}

