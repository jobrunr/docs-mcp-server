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

FROM azul/zulu-openjdk:21-jre-headless
# The Zulu image has no wget, which the HEALTHCHECK below needs.
RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*
# CVE-2026-91403: block the compromised intermediate CAs until the base image ships the fix.
COPY docker/cve-2026-91403-blocked.certs /tmp/
RUN echo >> "$JAVA_HOME/lib/security/blocked.certs" \
 && grep -v '^#' /tmp/cve-2026-91403-blocked.certs >> "$JAVA_HOME/lib/security/blocked.certs" \
 && rm /tmp/cve-2026-91403-blocked.certs
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
