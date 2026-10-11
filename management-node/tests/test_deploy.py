"""Tests for the DMP deployer (docker CLI via subprocess, mocked)."""

from __future__ import annotations

import pytest

from management_node.deploy import DmpDeployer, health_port_for


class _FakeProc:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def deployer(monkeypatch):
    calls: list[list[str]] = []

    def fake_run(cmd, capture_output=True, text=True, timeout=None):
        calls.append(cmd)
        if "rm" in cmd:
            return _FakeProc(0)
        if "run" in cmd:
            return _FakeProc(0, stdout="abc123")
        return _FakeProc(0)

    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr("subprocess.run", fake_run)
    d = DmpDeployer()
    d._calls = calls
    return d


def test_container_name(deployer):
    assert deployer.container_name("syntegonStopperingSystem") == "dmp-syntegonStopperingSystem"


def test_deploy_mounts_config_and_sets_heap(deployer):
    deployer.deploy("syntegonStopperingSystem", "/tmp/config")
    run_cmd = deployer._calls[-1]

    assert run_cmd[:3] == ["docker", "run", "-d"]
    assert "--name" in run_cmd and "dmp-syntegonStopperingSystem" in run_cmd
    assert "-v" in run_cmd and "/tmp/config:/usr/share/config:ro" in run_cmd
    # restart policy + per-asset health port (T1.7): two host-networked DMPs
    # must not collide on a fixed port.
    assert "--restart" in run_cmd and "unless-stopped" in run_cmd
    # the port hashes on the container name (ADR-021 unit naming includes the
    # AIMC-instance digest via its dmp- prefix)
    assert f"DATABRIDGE_HEALTH_PORT={health_port_for(deployer.container_name('syntegonStopperingSystem'))}" in run_cmd
    # Gson needs --add-opens to serialize Throwable (health DTO) on Java 17.
    assert "-e" in run_cmd and "JAVA_OPTS=-Xmx512m --add-opens java.base/java.lang=ALL-UNNAMED" in run_cmd
    assert "DMP_AFFORDANCE_VALIDATION=OFF" in run_cmd
    assert "--network" in run_cmd and "host" in run_cmd
    assert run_cmd[-1] == "eclipsebasyx/databridge:1.0.0-SNAPSHOT"


def test_affordance_validation_mode_is_forwarded_and_checked(deployer):
    deployer.affordance_validation = "REPORT"
    deployer.deploy("asset", "/tmp/config")
    assert "DMP_AFFORDANCE_VALIDATION=REPORT" in deployer._calls[-1]
    with pytest.raises(ValueError, match="DMP_AFFORDANCE_VALIDATION"):
        DmpDeployer(affordance_validation="INVALID")


def test_stop_removes_container(deployer):
    deployer.stop("syntegonStopperingSystem")
    assert deployer._calls[-1] == ["docker", "rm", "-f", "dmp-syntegonStopperingSystem"]


def test_health_ports_are_stable_and_distinct():
    a1 = health_port_for("assetA")
    assert a1 == health_port_for("assetA")
    assert 18086 <= a1 <= 18999
    ports = {health_port_for(f"asset{i}") for i in range(50)}
    assert len(ports) > 1


def test_deployer_resolves_port_collisions(monkeypatch):
    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr("subprocess.run", lambda cmd, **k: _FakeProc(0))
    # force both assets onto the same hashed port; probing must move the
    # second allocation to the next free port instead of double-binding.
    fixed = 18100
    monkeypatch.setattr("management_node.deploy.health_port_for", lambda name, lo=18086, hi=18999: fixed)
    d = DmpDeployer(pull_image=False)
    first = d._allocate_health_port("assetA")
    second = d._allocate_health_port("assetB")
    assert first == fixed
    assert second == fixed + 1
    d.release("assetA")
    assert d._allocate_health_port("assetC") == fixed


def test_deploy_times_out(monkeypatch):
    import subprocess as sp

    def slow_run(cmd, capture_output=True, text=True, timeout=None):
        raise sp.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr("subprocess.run", slow_run)
    from management_node.deploy import DockerError
    d = DmpDeployer(pull_image=False)
    with pytest.raises(DockerError, match="timed out"):
        d.deploy("assetA", "/tmp/cfg")
