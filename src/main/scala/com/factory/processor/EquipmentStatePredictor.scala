package com.factory.processor

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

object EquipmentStatePredictor {

  def predictState(
    temperature: Double,
    pressure: Double,
    vibration: Double,
    humidity: Double,
    equipmentType: String
  )(implicit spark: SparkSession): String = {
    // Load the saved model
    val model = PipelineModel.load("equipment_state_model")
    
    // Create a single-row DataFrame with the input features
    val inputData = spark.createDataFrame(Seq(
      (temperature, pressure, vibration, humidity, equipmentType)
    )).toDF("temperature", "pressure", "vibration", "humidity", "equipment_type")
    
    // Make prediction
    val prediction = model.transform(inputData)
    
    // Get the predicted state
    prediction.select("prediction").first().getDouble(0).toString
  }
}