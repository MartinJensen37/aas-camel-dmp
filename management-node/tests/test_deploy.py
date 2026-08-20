"""Tests for the DMP deployer (docker CLI via subprocess, mocked)."""

from __future__ import annotations

import pytest

from management_node.deploy import DmpDeployer


class _FakeProc:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def deployer(monkeypatch):
    calls: list[list[str]] = []

    def fake_run(cmd, capture_output=True, text=True):
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
    assert "-v" in run_cmd and "/tmp/config:/usr/share/config" in run_cmd
    # Gson needs --add-opens to serialize Throwable (health DTO) on Java 17.
    assert "-e" in run_cmd and "JAVA_OPTS=-Xmx512m --add-opens java.base/java.lang=ALL-UNNAMED" in run_cmd
    assert "--network" in run_cmd and "host" in run_cmd
    assert run_cmd[-1] == "eclipsebasyx/databridge:1.0.0-SNAPSHOT"


def test_stop_removes_container(deployer):
    deployer.stop("syntegonStopperingSystem")
    assert deployer._calls[-1] == ["docker", "rm", "-f", "dmp-syntegonStopperingSystem"]
