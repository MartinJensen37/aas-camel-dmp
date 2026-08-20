"""Per-asset DMP container lifecycle (SPEC §11.5, ADR-012).

Uses the ``docker`` CLI via subprocess (no SDK dependency; works for
compose-over-SSH / edge deployment). Each asset gets one DMP container with
its config directory mounted at ``/usr/share/config`` and a bounded JVM heap.
"""

from __future__ import annotations

import logging
import shutil
import subprocess

logger = logging.getLogger(__name__)


class DockerError(RuntimeError):
    pass


class DmpDeployer:
    def __init__(
        self,
        image: str = "eclipsebasyx/databridge:1.0.0-SNAPSHOT",
        *,
        jvm_opts: str = "-Xmx512m",
        network: str | None = "host",
        health_port: int = 18085,
    ) -> None:
        self.image = image
        self.jvm_opts = jvm_opts
        self.network = network
        self.health_port = health_port
        if shutil.which("docker") is None:
            raise DockerError("docker CLI not found on PATH")

    def container_name(self, asset_id_short: str) -> str:
        return f"dmp-{asset_id_short}"

    def deploy(self, asset_id_short: str, config_dir: str) -> str:
        """Start (or recreate) the DMP container for an asset.

        Idempotent: an existing container with the same name is replaced.
        Returns the container name.
        """
        name = self.container_name(asset_id_short)
        self._run(["docker", "rm", "-f", name], check=False)

        cmd = [
            "docker", "run", "-d",
            "--name", name,
            "-v", f"{config_dir}:/usr/share/config",
            "-e", f"JAVA_OPTS={self.jvm_opts}",
            # health endpoint on a dedicated port (the default 8085 collides
            # with the lab's dashboard-api when the DMP uses host networking)
            "-e", f"DATABRIDGE_HEALTH_PORT={self.health_port}",
        ]
        if self.network:
            cmd += ["--network", self.network]
        cmd.append(self.image)

        self._run(cmd)
        logger.info("deployed DMP container %s (image=%s, config=%s)", name, self.image, config_dir)
        return name

    def stop(self, asset_id_short: str) -> None:
        name = self.container_name(asset_id_short)
        self._run(["docker", "rm", "-f", name], check=False)
        logger.info("stopped DMP container %s", name)

    def _run(self, cmd: list[str], *, check: bool = True) -> str:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if check and proc.returncode != 0:
            raise DockerError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr}")
        return proc.stdout.strip()
