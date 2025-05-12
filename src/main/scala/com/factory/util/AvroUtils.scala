package com.factory.util

import com.factory.model._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import java.time.Instant

object AvroUtils {

  // This would need to be generated from Avro schemas in a real implementation
  def deserialize[T](bytes: Array[Byte], sensorType: String): T = {
    // In a real implementation, this would use proper Avro deserialization
    // with schema registry. This is a simplified mock implementation.
    sensorType match {
      case "pressure" =>
        Pressure(
          Instant.now().getEpochSecond,
          extractLabel(bytes, sensorType),
          PressureDataRecord(mockExtractDouble(bytes))
        ).asInstanceOf[T]

      case "temperature" =>
        Temperature(
          Instant.now().getEpochSecond,
          extractLabel(bytes, sensorType),
          TemperatureDataRecord(mockExtractDouble(bytes))
        ).asInstanceOf[T]

      case "flowRate" =>
        Humidity(
          Instant.now().getEpochSecond,
          extractLabel(bytes, sensorType),
          HumidityDataRecord(mockExtractDouble(bytes))
        ).asInstanceOf[T]

      case "noiseAndVibration" =>
        Vibration(
          Instant.now().getEpochSecond,
          extractLabel(bytes, sensorType),
          VibrationDataRecord(
            mockExtractDouble(bytes)
          )
        ).asInstanceOf[T]

      case _ => throw new IllegalArgumentException(s"Unsupported sensor type: $sensorType")
    }
  }

  // In a real implementation, this would extract the label from the Avro record
  def extractLabel(bytes: Array[Byte], sensorType: String): String = {
    // Mock implementation - in reality would parse the Avro record
    if (bytes.length > 0) {
      s"sensor-${bytes(0) % 5}"
    } else {
      "unknown"
    }
  }

  // Mock implementation to extract double values - in a real system this would parse the Avro record
  private def mockExtractDouble(bytes: Array[Byte], offset: Int = 0): Double = {
    if (bytes.length > offset) {
      bytes(offset).toDouble
    } else {
      0.0
    }
  }

  // UDF to serialize domain objects to Avro format
  def serializeToColumn(sensorType: String): Column = udf((obj: Any) => {
    // In a real implementation, this would use proper Avro serialization
    // with schema registry. This is a simplified mock implementation.
    Array[Byte](0, 1, 2, 3)
  }).apply(col("*"))

  // UDF for serializing Event objects
  val serializeEventToColumn: Column = udf((event: Event) => {
    // In a real implementation, this would use proper Avro serialization
    // with schema registry. This is a simplified mock implementation.
    Array[Byte](0, 1, 2, 3)
  }).apply(col("*"))
}