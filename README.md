# Spark Data Processor

## Description

This module is responsible for processing raw data from input Kafka topics and publishing results to output topics.
It uses Apache Spark Structured Streaming to process those data.

This module computes the following result types:
* sensor measurements mean from given time window
* reaction performance
* threshold-based event detection and generation

## Technology Stack

* Apache Spark - for distributed stream processing
* Scala - as the programming language
* Maven - as the build system
* Kafka - as the message broker
* Avro - for message serialization

## Architecture

The application is structured as a set of Spark Structured Streaming jobs that:
1. Read data from Kafka topics
2. Process the data using time-windowed aggregations, stream joins, or threshold filters
3. Write results back to Kafka topics

### Mean Calculation

For each sensor type (pressure, temperature, etc.), the application:
- Reads data from an input Kafka topic
- Groups them by window (time-based) and sensor label
- Calculates the mean value across that window
- Publishes the result to an output Kafka topic

### Performance Calculation

For the reaction performance:
- Reads both input and output gas composition data from separate Kafka topics
- Joins the streams using time-based windows
- Calculates the performance based on the efficiency of the chemical reaction (Haber process)
- Publishes the performance results to a Kafka topic

### Event Processing

For each sensor type (pressure, temperature, etc.), the application:
- Reads data from input Kafka topics
- Checks if sensor readings exceed configurable thresholds
- Generates events when thresholds are crossed
- Publishes events to an output Kafka topic

## Configuration

### Mean 

#### Enabled
* `mean.{sensor-type}.enabled`
* Determines if computing mean of given sensor type is enabled

#### Window size
* `mean.{sensor-type}.windowSize`
* Specifies collecting window size in seconds 

#### Labels
* `mean.{sensor-type}.labels`
* Specifies for which labels mean should be computed of given sensor type

#### Input topic
* `mean.{sensor-type}.inputTopic`
* Name of input Kafka topic

#### Output topics postfix
* `mean.{sensor-type}.outputTopicsPostfix`
* Each sensor label has its own topic. This property specifies postfix, so final topic name will be like this: "${label}${postfix}"

#### Debug enabled
* `mean.{sensor-type}.debugEnabled`
* Specifies if debug messages are present for given sensor type

Available {sensor-type} values:
* pressure
* temperature
* flowRate
* gasComposition
* noiseAndVibration

### Performance

#### Enabled
* `performance.reaction.enabled`
* Determines if computing performance is enabled

#### Window size
* `performance.reaction.joinWindowsDurationMillis`
* Specifies collecting window size in milliseconds 

#### Input topic
* `performance.reaction.inputTopic`
* Specifies name of Kafka topic which collects data before reactor

#### Output topic
* `performance.reaction.outputTopic`
* Specifies name of Kafka topic which collects data after reactor

#### Result topic
* `performance.reaction.resultTopic`
* Specifies name of Kafka topic where performance messages are published

#### Debug enabled
* `performance.reaction.debugEnabled`
* Specifies if debug messages are present for performance processing

### Events

#### Enabled
* `events.enabled`
* Determines if event processing is enabled

#### Result topic
* `events.resultTopic`
* Specifies name of Kafka topic where all events are published

#### Sensor-specific configuration
* `events.{sensor-type}.enabled`
* Determines if events for this sensor type are enabled

* `events.{sensor-type}.inputTopic`
* Specifies name of Kafka topic used as input for event detection

* `events.{sensor-type}.threshold`
* Specifies the threshold value that triggers an event

* `events.{sensor-type}.debugEnabled`
* Specifies if debug messages are present for event processing

## Building

```bash
mvn clean package
```

## Running

```bash
spark-submit --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.confluent:kafka-avro-serializer:7.3.0 \
  --class com.factory.SparkDataProcessor \
  target/spark-data-processor-1.0.0.jar
```

## Docker

```bash
# Build the application
mvn clean package

# Build the Docker image
docker build -t spark-data-processor .

# Run the container
docker run -d spark-data-processor
```

