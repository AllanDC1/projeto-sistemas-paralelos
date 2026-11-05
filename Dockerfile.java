FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /workspace

COPY client/pom.xml client/pom.xml
COPY client/src client/src
COPY protocol/chat.proto protocol/chat.proto
RUN mvn -f client/pom.xml --batch-mode package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /workspace/client/target/bot.jar /app/bot.jar

ENTRYPOINT ["java", "-jar", "/app/bot.jar"]