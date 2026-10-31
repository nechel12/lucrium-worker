"""Тесты построения команды bubblewrap для плагинов."""

from __future__ import annotations

from pathlib import Path

from lucrium_worker.plugins.host import _sandbox_command


def _prepare(tmp_path: Path) -> tuple[Path, Path, Path]:
    data = tmp_path / "data"
    plugin_dir = data / "plugins" / "test-plugin"
    sockets = data / "sockets"
    plugin_dir.mkdir(parents=True)
    sockets.mkdir(parents=True)
    return data, plugin_dir, sockets


def test_sandbox_command_hides_data_and_config(tmp_path: Path) -> None:
    data, plugin_dir, sockets = _prepare(tmp_path)
    config = tmp_path / "etc" / "lucrium"
    config.mkdir(parents=True)

    command = _sandbox_command(
        python="/usr/bin/python3",
        plugin_dir=plugin_dir,
        sockets_dir=sockets,
        data_dir=data,
        config_dir=config,
    )

    assert command[0] == "bwrap"
    assert "--unshare-all" in command
    assert command[command.index(str(data)) - 1] == "--tmpfs"
    assert command[command.index(str(config)) - 1] == "--tmpfs"
    assert str(plugin_dir) in command
    assert str(sockets) in command
    assert command[-4:] == ["--", "/usr/bin/python3", "-m", "lucrium_plugin_sdk"]


def test_sandbox_never_shares_host_network(tmp_path: Path) -> None:
    """``permissions.network`` — это ctx.http в воркере, а не сеть внутри песочницы."""
    data, plugin_dir, sockets = _prepare(tmp_path)
    command = _sandbox_command(
        python="/usr/bin/python3",
        plugin_dir=plugin_dir,
        sockets_dir=sockets,
        data_dir=data,
        config_dir=None,
    )
    assert "--share-net" not in command
    assert "--chdir" in command
