package com.factory.processor

import com.factory.config.{KafkaConfig, MeanConfig}
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.spark.sql.{Dataset, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import java.sql.Timestamp
import java.nio.ByteBuffer
import org.apache.avro.{Schema => AvroSchema}

class EquipmentStateProcessor(
  spark: SparkSession,
  kafkaConfig: KafkaConfig
) {
  private val logger = LoggerFactory.getLogger(getClass)
  import spark.implicits._

  private lazy val restService = new RestService(kafkaConfig.schemaRegistryUrl)

  private val pressureOutputAvroSchemaJson = restService.getLatestVersion("augumented-pressure-value")
  private val temperatureOutputAvroSchemaJson = restService.getLatestVersion("augumented-temperature-value")
  private val humidityOutputAvroSchemaJson = restService.getLatestVersion("augumented-humidity-value")
  private val vibrationOutputAvroSchemaJson = restService.getLatestVersion("augumented-vibration-value")

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
      .select(from_avro(expr("substring(value, 6)"), avroSchemaJson).as("avro_data")) // Strip header for input
      .select(
        col("avro_data.label").as("label"),
        (col("avro_data.timestamp") / 1000).cast("timestamp").as("eventTime"), // ms to Spark Timestamp
        col(s"avro_data.data.$valueFieldName").cast("float").as(valueFieldName)
      )
  }

  def start(): Unit = {
    logger.info("Starting Equipment State Processor")

    val watermarkDuration = "1 minute"

    // Fetch input schemas dynamically
    val pressureEvents = readAndPrepareSensorStream("pressure", "Pressure", "pressure")
      .withWatermark("eventTime", watermarkDuration).alias("p")
    val temperatureEvents = readAndPrepareSensorStream("temperature", "Temperature", "temperature")
      .withWatermark("eventTime", watermarkDuration).alias("t")
    val humidityEvents = readAndPrepareSensorStream("humidity", "Humidity", "humidity")
      .withWatermark("eventTime", watermarkDuration).alias("h")
    val vibrationEvents = readAndPrepareSensorStream("vibration", "Vibration", "vibration")
      .withWatermark("eventTime", watermarkDuration).alias("v")

    val joinedData = pressureEvents
      .join(temperatureEvents, expr("p.label = t.label AND p.eventTime = t.eventTime"), "inner")
      .join(humidityEvents, expr("p.label = h.label AND p.eventTime = h.eventTime"), "inner")
      .join(vibrationEvents, expr("p.label = v.label AND p.eventTime = v.eventTime"), "inner")
      .select(
        col("p.label").as("equipmentId"),
        col("p.eventTime").as("event_timestamp"), // Spark TimestampType
        col("p.pressure").as("pressure_value"),
        col("t.temperature").as("temperature_value"),
        col("h.humidity").as("humidity_value"),
        col("v.vibration").as("vibration_value")
      )

    val predictionsDF = joinedData
      .map { row =>
        val equipmentId = row.getAs[String]("equipmentId")
        val eventTimestamp = row.getAs[Timestamp]("event_timestamp")
        val pressure = row.getAs[Float]("pressure_value")
        val temperature = row.getAs[Float]("temperature_value")
        val humidity = row.getAs[Float]("humidity_value")
        val vibration = row.getAs[Float]("vibration_value")

        implicit val SESSIONS: SparkSession = spark
        val state = EquipmentStatePredictor.predictState(
          temperature = temperature.toDouble, pressure = pressure.toDouble,
          vibration = vibration.toDouble, humidity = humidity.toDouble,
          equipmentType = equipmentId
        )
        (equipmentId, eventTimestamp.getTime(), pressure, temperature, humidity, vibration, state) // timestamp as Long (ms)
      }
      .toDF("equipmentId", "timestamp_ms", "pressure", "temperature", "humidity", "vibration", "predictedState")

    // Publish augmented data to separate Avro topics
    val sensorTypes = Seq(
      ("pressure", pressureOutputAvroSchemaJson, "Pressure"),
      ("temperature", temperatureOutputAvroSchemaJson, "Temperature"),
      ("humidity", humidityOutputAvroSchemaJson, "Humidity"),
      ("vibration", vibrationOutputAvroSchemaJson, "Vibration")
    )

    sensorTypes.foreach { case (sensorName, outputSchemaJson, baseOutputName) =>
      
      val outputSubjectName = s"com.factory.message.augmented.${baseOutputName}WithState-value"
      val outputTopicName = s"${sensorName}_with_state"
      
      logger.info(s"Setting up output for $outputTopicName with subject $outputSubjectName")

      // Fetch schema ID for output Avro schema from SR
      val schemaId = restService.getLatestVersion(outputSubjectName).getId()

      val dfForAvro = predictionsDF.select(
        struct(
          col("equipmentId").as("label"),
          col("timestamp_ms").as("timestamp"), // Ensure this is long for Avro
          // Struct for sensor data, e.g., data: {pressure: <value>}
          struct(col(sensorName).as(sensorName)).as("data"), 
          col("predictedState")
        ).as("payload") // This 'payload' struct must match the Avro schema
      )

      val finalOutputDf = dfForAvro
        .select(
          col("payload.label").as("key"), // Kafka message key
          addSchemaRegistryHeaderUDF(schemaId)(to_avro(col("payload"), outputSchemaJson.getSchema)).as("value") // Kafka message value
        )
      
      finalOutputDf.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
        .option("topic", outputTopicName)
        .trigger(Trigger.ProcessingTime("1 minute"))
        .start()
      
      logger.info(s"Started Kafka output stream for $outputTopicName")
    }
    logger.info(s"All Equipment State Prediction output streams to Kafka started.")
  }
} 