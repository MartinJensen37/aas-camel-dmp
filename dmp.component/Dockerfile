FROM eclipse-temurin:21-jre

# Install wget to check health status
RUN apt update && apt install -y wget && apt clean

# Copy built jar to image using the jar name specified in the pom.xml (JAR_FILE)
ARG JAR_FILE=target/*.jar

COPY ${JAR_FILE} /usr/share/databridgeExecutable.jar
COPY target/lib /usr/share/lib

# This is a process-wide bound, shared by all mappings and Lua blobs.
# Override only after sizing the intended adoption scope.
ENV JAVA_OPTS="-Xmx512m"

# Shared Operations/health/status listener.
EXPOSE 8080

# Forward stop signals to the JVM for bounded managed shutdown.
CMD exec java $JAVA_OPTS -jar "/usr/share/databridgeExecutable.jar"
