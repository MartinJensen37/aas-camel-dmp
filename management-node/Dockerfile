# Management node container (SPEC §11, ADR-013 — Python orchestrator).
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY management_node ./management_node

# confluent-kafka needs librdkafka; install the package without build isolation
# so the wheels are used
RUN pip install --no-cache-dir .

# The management node talks to Kafka + the AAS REST API and drives the docker
# CLI (mounted from the host) — see README for env vars.
CMD ["python", "-m", "management_node"]
