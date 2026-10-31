"""Изоляция плагинов: песочница обязана быть включена, сокет — принят только от своего процесса."""

from __future__ import annotations

import os
import socket as socket_module
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from lucrium_worker.logs import LogBuffer
from lucrium_worker.plugins.host import (
    PluginHost,
    _assert_public_url,
    _parent_pid,
    _peer_credentials,
    _sandbox_command,
)
from lucrium_worker.storage import Storage

from .conftest import FakeSessions


def _sandbox_kwargs(tmp_path: Path) -> dict[str, object]:
    plugin_dir = tmp_path / "data" / "plugins" / "test-plugin"
    plugin_dir.mkdir(parents=True)
    return {
        "python": "/usr/bin/python3",
        "plugin_dir": plugin_dir,
        "sockets_dir": tmp_path / "data" / "sockets" / "test-plugin",
        "data_dir": tmp_path / "data",
        "config_dir": None,
    }


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8443/",
        "http://localhost:8443/",
        "http://10.0.0.5/status",
        "http://172.16.0.1/",
        "http://192.168.1.10/admin",
        "http://169.254.169.254/latest/meta-data/",
        "http://[::1]/",
        "http://[::ffff:127.0.0.1]/",
    ],
)
def test_egress_refuses_internal_addresses(url: str) -> None:
    """Плагин не должен через ctx.http лезть внутрь VPS и в метаданные хостинга."""
    with pytest.raises(ValueError, match="not a public address"):
        _assert_public_url(url)


def test_egress_refuses_non_http_and_hostless_urls() -> None:
    with pytest.raises(ValueError, match="must be http"):
        _assert_public_url("file:///etc/passwd")
    with pytest.raises(ValueError, match="must be http"):
        _assert_public_url("gopher://8.8.8.8/")
    with pytest.raises(ValueError, match="no host"):
        _assert_public_url("http:///x")


@pytest.mark.parametrize("url", ["http://8.8.8.8/", "https://1.1.1.1/x", "https://140.82.121.4/"])
def test_egress_allows_public_addresses(url: str) -> None:
    _assert_public_url(url)


def test_sandbox_command_requires_the_data_directory(tmp_path: Path) -> None:
    kwargs = _sandbox_kwargs(tmp_path)
    kwargs["data_dir"] = tmp_path / "absent"
    with pytest.raises(RuntimeError, match="data directory"):
        _sandbox_command(**kwargs)  # type: ignore[arg-type]


def test_sandbox_command_requires_the_config_directory(tmp_path: Path) -> None:
    kwargs = _sandbox_kwargs(tmp_path)
    kwargs["config_dir"] = tmp_path / "absent-config"
    with pytest.raises(RuntimeError, match="config directory"):
        _sandbox_command(**kwargs)  # type: ignore[arg-type]


def test_hidden_mounts_are_always_applied(tmp_path: Path) -> None:
    command = _sandbox_command(**_sandbox_kwargs(tmp_path))  # type: ignore[arg-type]
    data = str(tmp_path / "data")
    assert command[command.index(data) - 1] == "--tmpfs"


def test_host_without_sandbox_cannot_require_it(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "lucrium.db", tmp_path / "secret.key")
    logs = LogBuffer()
    host = PluginHost(
        plugins_dir=tmp_path / "plugins",
        sockets_dir=tmp_path / "sockets",
        sessions=FakeSessions(),  # type: ignore[arg-type]
        storage=storage,
        logs=logs,
        on_error=lambda *_: None,
        on_change=lambda: None,
        sandbox=False,
    )
    assert host.sandbox_required is False
    default_host = PluginHost(
        plugins_dir=tmp_path / "plugins",
        sockets_dir=tmp_path / "sockets",
        sessions=FakeSessions(),  # type: ignore[arg-type]
        storage=storage,
        logs=logs,
        on_error=lambda *_: None,
        on_change=lambda: None,
    )
    # конструктор по умолчанию не должен молча разрешать запуск без изоляции
    assert default_host.sandbox_required is True or not default_host.sandbox_available


@pytest.mark.skipif(sys.platform not in {"linux", "linux2"}, reason="SO_PEERCRED is Linux-only")
def test_peer_credentials_report_the_other_end() -> None:
    left, right = socket_module.socketpair(socket_module.AF_UNIX, socket_module.SOCK_STREAM)
    with left, right:
        writer = SimpleNamespace(get_extra_info=lambda name: left)
        credentials = _peer_credentials(writer)  # type: ignore[arg-type]
        assert credentials is not None
        pid, uid = credentials
        # на socketpair «пир» — мы сами; pid нулевым быть не может
        assert pid == os.getpid()
        assert uid == os.getuid()
        # а мёртвое соединение не должно ронять проверку
        assert _peer_credentials(SimpleNamespace(get_extra_info=lambda _name: None)) is None
        assert _parent_pid(os.getpid()) == os.getppid()


@pytest.mark.skipif(sys.platform not in {"linux", "linux2"}, reason="procfs is Linux-only")
def test_parent_pid_of_unknown_process_is_none() -> None:
    assert _parent_pid(2**30) is None
