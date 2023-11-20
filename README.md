# Kafka data processor

## Description

This module is responsible for raw data processing from input Kafka topic and publishing results to output topics.
This module uses Kafka Streams to process those data. 

This module computes the following result types:
* sensor measurements mean from given time window
* reaction performance

## Configuration

### Mean 

#### Enabled
* spring.kafka.streams.config.mean.{sensor-type}.enabled
* Determines if computing mean of given sensor type is enabled

#### Window size
* spring.kafka.streams.config.mean.{sensor-type}.windowSize
* Specifies collecting window size in seconds 

#### Labels
* spring.kafka.streams.config.mean.{sensor-type}.labels
* Specifies for which labels mean should be computed of given sensor type

#### Input topic
* spring.kafka.streams.config.mean.{sensor-type}.labels
* Name of input Kafka topic

#### Output topics postfix
* spring.kafka.streams.config.mean.{sensor-type}.outputTopicsPostfix
* Each sensor label has its own topic. This property specifies postfix, so final topic name will be like this: "${label}${postfix}"

#### Debug enabled
* spring.kafka.streams.config.mean.{sensor-type}.debugEnabled
* Specifies if debug messages are present for given sensor type

Available {sensor-type} values:
* pressure
* temperature
* flowRate
* gasComposition
* compressorState
* noiseAndVibration

### Performance

#### Enabled
* spring.kafka.streams.config.performance.reaction.enabled
* Determines if computing performance is enabled

#### Window size
* spring.kafka.streams.config.performance.reaction.joinWindowsDurationMillis
* Specifies collecting window size in milliseconds 

#### Input topic
* spring.kafka.streams.config.performance.reaction.inputTopic
* Specifies name of Kafka topic which collects data before reactor

#### Output topic
* spring.kafka.streams.config.performance.reaction.outputTopic
* Specifies name of Kafka topic which collects data after reactor

#### Result topic
* spring.kafka.streams.config.performance.reaction.resultTopic
* Specifies name of Kafka topic where performance messages are published

#### Debug enabled
* spring.kafka.streams.config.performance.reaction.debugEnabled
* Specifies if debug messages are present for performance processing

