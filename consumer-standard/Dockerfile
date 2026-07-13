# ---- build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY common ./common
COPY producer ./producer
COPY consumer-standard ./consumer-standard
COPY consumer-streams ./consumer-streams
RUN mvn -q -pl consumer-standard -am -DskipTests clean package

# ---- runtime ----
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/consumer-standard/target/consumer-standard.jar app.jar
ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-jar", "app.jar"]
