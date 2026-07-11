# offboarding — Helidon 4 SE on the JDK (virtual threads), one jar plus its libs/ classpath
# (built on the host). The jar's manifest Class-Path points at libs/, so it rides next to the jar.
FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
COPY target/microservice-offboarding.jar app.jar
COPY target/libs libs
EXPOSE 8094
CMD ["java", "-jar", "app.jar"]
