# Use a base image with OpenJDK 19
FROM openjdk:19-ea-19-jdk-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled JAR file to the container
COPY target/indexer_csv-1.0-SNAPSHOT.jar ./indexer_csv-module.jar

# Expose Hazelcast default port
EXPOSE 5701

# Default empty JAVA_OPTS
ENV JAVA_OPTS=""

# Command to run the application with JAVA_OPTS
CMD java $JAVA_OPTS -jar indexer_csv-module.jar
