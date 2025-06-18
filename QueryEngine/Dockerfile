# Use a base image with OpenJDK 19
FROM openjdk:19-ea-19-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the built JAR file into the container
COPY target/queryengine-1.0-SNAPSHOT.jar /app/query-engine.jar

# Use an environment variable to set the entry point
ARG ENTRY_POINT
ENV ENTRY_POINT=${ENTRY_POINT}

# Specify the entry point for the application
ENTRYPOINT ["java", "-jar", "/app/query-engine.jar"]
