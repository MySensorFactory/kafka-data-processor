package com.factory.model

import java.time.Instant

trait SensorReading {
  def timestamp: Long
  def label: String
}

case class PressureDataRecord(pressure: Double)
case class Pressure(timestamp: Long, label: String, data: PressureDataRecord) extends SensorReading

case class TemperatureDataRecord(temperature: Double)
case class Temperature(timestamp: Long, label: String, data: TemperatureDataRecord) extends SensorReading

case class HumidityDataRecord(humidity: Double)
case class Humidity(timestamp: Long, label: String, data: HumidityDataRecord) extends SensorReading

case class VibrationDataRecord( vibration: Double)
case class Vibration(timestamp: Long, label: String, data: VibrationDataRecord) extends SensorReading

case class Event(
  timestamp: Long,
  sensorType: String,
  sensorId: String,
  value: Double,
  threshold: Double,
  title: String,
  isAlert: Boolean
)