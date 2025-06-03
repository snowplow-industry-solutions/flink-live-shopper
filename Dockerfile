# Stage 1: Build the application using Gradle
FROM gradle:8.5-jdk21 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy Gradle wrapper files
COPY gradlew .
COPY gradle ./gradle/
COPY settings.gradle .

# Copy the Flink application module source code
COPY apps/flink ./apps/flink/

# Grant execution permission to gradlew
RUN chmod +x ./gradlew

# Build the shadow JAR (this will also download dependencies if needed)
RUN ./gradlew :apps:flink:shadowJar --no-daemon

# Stage 2: Create the final Flink image for Application Mode
FROM flink:2.0.0-java21

# Copy the built JAR from the builder stage into the directory Flink scans
COPY --from=builder /app/apps/flink/build/libs/*-all.jar /opt/flink/usrlib/flink-app.jar

# CMD removed - command will be provided by docker-compose
# CMD ["standalone-job"] 
