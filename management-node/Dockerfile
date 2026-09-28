# Management node container (SPEC §11, ADR-013 — Python orchestrator).
#
# The shared AAS data-model package (ADR-016, `aas-model`) is not on a public
# index yet, so the build vendors it (T1.8). Before building, make the sibling
# checkout available next to this Dockerfile:
#
#     cp -r ../../aas-model ./_aas_model        # or: ln -s ../../aas-model _aas_model
#
# then:
#
#     docker build -t dmp-management-node .
#
# A private index carrying aas-model>=0.2 can be used instead by pointing
# --build-arg PIP_EXTRA_INDEX_URL=<url> at it and installing that version over
# the vendored one.
FROM python:3.12-slim

ARG PIP_EXTRA_INDEX_URL=""

WORKDIR /app

# The deployer drives the HOST docker daemon through the mounted socket and
# needs the docker CLI on PATH (static build; no engine inside the container).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz \
       | tar -xz -C /tmp docker/docker \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && chmod +x /usr/local/bin/docker \
    && rm -rf /tmp/docker

COPY pyproject.toml README.md ./
COPY management_node ./management_node

COPY _aas_model/ ./_aas_model/

# confluent-kafka needs librdkafka; install the package without build isolation
# so the wheels are used. The vendored model installs first so the
# `aas-model>=0.2` dependency pin resolves locally.
RUN pip install --no-cache-dir \
        ${PIP_EXTRA_INDEX_URL:+--extra-index-url=${PIP_EXTRA_INDEX_URL}} \
        ./_aas_model \
    && pip install --no-cache-dir .

# The management node talks to Kafka + the AAS REST API and drives the docker
# CLI (mounted from the host) — see README for env vars.
CMD ["python", "-m", "management_node"]
