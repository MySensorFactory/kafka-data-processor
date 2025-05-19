FROM apache/spark:3.5.5

USER root

RUN mkdir -p /opt/spark/app-jars
RUN mkdir -p /tmp/spark_checkpoints_eventproc
RUN mkdir -p /tmp/spark-checkpoints
RUN chown -R 185:185 /opt/spark/app-jars && chmod 777 /opt/spark/app-jars
RUN chown -R 185:185 /tmp/spark-checkpoints && chmod 777 /tmp/spark-checkpoints

USER spark

# Copy the application jar file and resources
COPY target/spark-data-processor-1.0.0.jar /opt/spark/app-jars/spark-data-processor.jar
COPY src/main/resources/application.conf /opt/spark/app-jars/application.conf
COPY src/main/resources/equipment_state_spark_model /opt/spark/app-jars/equipment_state_spark_model

# No CMD or ENTRYPOINT! The Spark Operator will handle this.