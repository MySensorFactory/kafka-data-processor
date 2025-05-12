FROM apache/spark:3.5.5

USER root

RUN mkdir -p /opt/spark/app-jars

USER spark

# Copy the application jar file and resources
COPY target/spark-data-processor-1.0.0.jar /opt/spark/app-jars/spark-data-processor.jar
COPY src/main/resources/application.conf /opt/spark/app-jars/application.conf

# No CMD or ENTRYPOINT! The Spark Operator will handle this.