# offboarding — Helidon 4 SE on the JDK (virtual threads), one jar plus its libs/ classpath
# (built on the host). The jar's manifest Class-Path points at libs/, so it rides next to the jar.
#
# The paths are offboarding-boundary/target/... because the service is three Maven modules
# (Boundary-Control-Entity) and the boundary is the one with a main(). It keeps the old
# artifact name, so only these two lines had to know about the split.
FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
COPY offboarding-boundary/target/microservice-offboarding.jar app.jar
COPY offboarding-boundary/target/libs libs
EXPOSE 8094
CMD ["java", "-jar", "app.jar"]
