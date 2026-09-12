# offboarding — Helidon 4 SE on the JDK (virtual threads), one jar plus its libs/ classpath
# (built on the host). The jar's manifest Class-Path points at libs/, so it rides next to the jar.
#
# The paths are offboarding-infrastructure/target/... because the service is four Maven modules
# (the estate's layers: domain, system, application, infrastructure) and infrastructure is the one
# with a main(). It keeps the old artifact name, so only these two lines had to learn about it.
FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
COPY offboarding-infrastructure/target/microservice-offboarding.jar app.jar
COPY offboarding-infrastructure/target/libs libs
EXPOSE 8094
CMD ["java", "-jar", "app.jar"]
