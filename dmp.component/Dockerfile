FROM eclipse-temurin:17-jre

# Install wget to check health status
RUN apt update && apt install -y wget && apt clean

# Copy built jar to image using the jar name specified in the pom.xml (JAR_FILE)
ARG JAR_FILE=target/*.jar

COPY ${JAR_FILE} /usr/share/databridgeExecutable.jar
COPY target/lib /usr/share/lib

# DMP config directory: routes.json, <datasource>/*transformers.json and
# queries/*.lua (AIMC transformation blobs) are mounted here at runtime
# (SPEC §9.2 — the DataBridgeExecutable reads /usr/share/config by default).
VOLUME /usr/share/config

# JVM sizing: -Xmx is the effective per-blob memory bound (SPEC §9.3). Each
# DMP is one process per asset, so a misbehaving Lua blob can only OOM its own
# container (per-asset fault isolation). Override via JAVA_OPTS if needed.
ENV JAVA_OPTS="-Xmx512m"

# Expose this port for health check
EXPOSE 8085

# Start the jar
CMD java $JAVA_OPTS -jar "/usr/share/databridgeExecutable.jar"
