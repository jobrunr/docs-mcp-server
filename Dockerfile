FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
RUN mvn -B -q dependency:go-offline
COPY src ./src
# Spring Boot 4 removed the `layertools` jarmode; `tools extract --layers` replaces it
# and produces the same dependencies/spring-boot-loader/snapshot-dependencies/application split.
RUN mvn -B -q -DskipTests package \
 && mkdir -p target/extracted \
 && java -Djarmode=tools -jar target/jobrunr-docs-mcp-*.jar extract --layers --launcher --destination target/extracted

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
RUN useradd -r -u 1001 app && mkdir -p /app/data && chown -R app:app /app
USER app
COPY --from=build /workspace/target/extracted/dependencies/ ./
COPY --from=build /workspace/target/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/target/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/target/extracted/application/ ./
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=50 -XX:+UseSerialGC -Djava.security.egd=file:/dev/./urandom"
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
