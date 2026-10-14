FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN chmod +x mvnw
COPY src src
RUN ./mvnw --batch-mode -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/ledgerflow-backend-0.0.1-SNAPSHOT.jar /app/ledgerflow.jar
USER 10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/ledgerflow.jar"]
