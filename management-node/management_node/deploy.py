"""Per-asset DMP container lifecycle (SPEC §11.5, ADR-012, T1.7).

Uses the ``docker`` CLI via subprocess (no SDK dependency; works for
compose-over-SSH / edge deployment). Each asset gets one DMP container with
its config directory mounted read-only at ``/usr/share/config`` and a bounded
JVM heap.

Health ports: the core honours ``DATABRIDGE_HEALTH_PORT``, but a fixed port
clashes as soon as two units deploy with host networking. The port is
therefore derived deterministically from the DMP unit name (itself hashed on
the AIMC instance id, ADR-021) into 18086–18999 and collision-checked against
the ports of units deployed by this process at run time.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import subprocess
import threading

logger = logging.getLogger(__name__)

#: Subprocess ceiling — docker commands must never hang the reconcile thread.
DOCKER_TIMEOUT_S = 60.0

#: Health-port allocation range (below the dashboard-api's 18085 default is
#: avoided; range leaves room for ~900 concurrent per-unit DMPs).
HEALTH_PORT_MIN = 18086
HEALTH_PORT_MAX = 18999


class DockerError(RuntimeError):
    pass


def health_port_for(unit_name: str,
                    lo: int = HEALTH_PORT_MIN, hi: int = HEALTH_PORT_MAX) -> int:
    """Deterministic per-unit health port (stable across restarts)."""
    digest = hashlib.sha256(unit_name.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big")
    return lo + value % (hi - lo + 1)


class DmpDeployer:
    def __init__(
        self,
        image: str = "eclipsebasyx/databridge:1.0.0-SNAPSHOT",
        *,
        # Gson needs --add-opens to serialize Throwable (health DTO) on Java 17.
        jvm_opts: str = "-Xmx512m --add-opens java.base/java.lang=ALL-UNNAMED",
        network: str | None = "host",
        health_port: int | None = None,
        timeout: float = DOCKER_TIMEOUT_S,
        pull_image: bool = True,
        affordance_validation: str = "OFF",
    ) -> None:
        if affordance_validation not in {"OFF", "ON", "REPORT"}:
            raise ValueError("DMP_AFFORDANCE_VALIDATION must be OFF, ON, or REPORT")
        self.affordance_validation = affordance_validation
        self.image = image
        self.jvm_opts = jvm_opts
        self.network = network
        #: Fixed override; None -> deterministic per-unit hash port.
        self.health_port = health_port
        self.timeout = timeout
        self.pull_image = pull_image
        self._ports_lock = threading.Lock()
        self._assigned_ports: dict[str, int] = {}
        if shutil.which("docker") is None:
            raise DockerError("docker CLI not found on PATH")

    def container_name(self, unit_name: str) -> str:
        return f"dmp-{unit_name}" if not unit_name.startswith("dmp-") else unit_name

    def _allocate_health_port(self, unit_name: str) -> int:
        """Stable hashed port, collision-resolved among concurrently assigned
        units (linear probing in hash order keeps assignments stable)."""
        with self._ports_lock:
            if unit_name in self._assigned_ports:
                return self._assigned_ports[unit_name]
            used = set(self._assigned_ports.values())
            port = health_port_for(unit_name)
            while port in used and port < HEALTH_PORT_MAX:
                port += 1
            if port in used:
                raise DockerError(
                    f"no free health port in [{HEALTH_PORT_MIN}, {HEALTH_PORT_MAX}] "
                    f"for {unit_name}")
            self._assigned_ports[unit_name] = port
            return port

    def release(self, unit_name: str) -> None:
        """Frees the unit's health-port reservation after decommissioning."""
        with self._ports_lock:
            self._assigned_ports.pop(unit_name, None)

    def deploy(self, unit_name: str, config_dir: str, *,
               container_name: str | None = None) -> str:
        """Start (or recreate) the DMP container for a unit.

        Idempotent: an existing container with the same name is replaced.
        ``container_name`` overrides the derived name (the {delegation_base}
        DNS convention: dmp-<aas id_short lowercased> — the aas-env's
        operation delegation resolves it). Returns the container name.
        """
        name = container_name or self.container_name(unit_name)
        self._run(["docker", "rm", "-f", name], check=False)
        if self.pull_image:
            # explicit pull before run: stale local images otherwise mask
            # upstream fixes; failures fall back to the local image.
            self._run(["docker", "pull", self.image], check=False)

        port = self.health_port or self._allocate_health_port(name)

        cmd = [
            "docker", "run", "-d",
            "--name", name,
            "--restart", "unless-stopped",
            "-v", f"{config_dir}:/usr/share/config:ro",
            "-e", f"JAVA_OPTS={self.jvm_opts}",
            "-e", f"DMP_AFFORDANCE_VALIDATION={self.affordance_validation}",
            # health endpoint on a dedicated per-unit port (fixed 18085 made
            # a second host-networked container fail to bind)
            "-e", f"DATABRIDGE_HEALTH_PORT={port}",
        ]
        if self.network:
            cmd += ["--network", self.network]
            # network-mode deployments: the in-container REST/health surface
            # stays on 8080 so the {delegation_base} DNS convention
            # (dmp-<aas id>:8080) resolves; the hashed port is the PUBLISHED
            # host port for host-side callers (curl/Postman)
            cmd += ["-e", "DATABRIDGE_HEALTH_PORT=8080"]
            cmd += ["-p", f"{port}:8080"]
        cmd.append(self.image)

        self._run(cmd)
        logger.info("deployed DMP container %s (image=%s, config=%s, health=%d%s)",
                    name, self.image, config_dir, port,
                    ", restart=unless-stopped")
        return name

    def stop(self, unit_name: str, *,
             container_name: str | None = None) -> None:
        name = container_name or self.container_name(unit_name)
        self._run(["docker", "rm", "-f", name], check=False)
        self.release(name)
        logger.info("stopped DMP container %s", name)

    def _run(self, cmd: list[str], *, check: bool = True) -> str:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=self.timeout)
        except subprocess.TimeoutExpired as e:
            raise DockerError(f"command timed out after {self.timeout}s: "
                              f"{' '.join(cmd)}") from e
        if check and proc.returncode != 0:
            raise DockerError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr}")
        return proc.stdout.strip()
