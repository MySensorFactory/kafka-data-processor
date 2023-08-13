FROM eclipse-temurin:17-jdk-jammy AS build

ARG SERVICE_NAME
ARG DIRECTORY

COPY $DIRECTORY/$SERVICE_NAME.jar /home/service.jar
WORKDIR /home/
CMD java -jar service.jar