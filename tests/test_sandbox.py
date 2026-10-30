"""Тесты построения команды bubblewrap для плагинов."""

from __future__ import annotations

from pathlib import Path

from lucrium_worker.plugins.host import _sandbox_command


def test_sandbox_command_hides_data_and_config(tmp_path: Path) -> None:
    data = tmp_path / "data"
    plugins = data / "plugins"
    sockets = data / "sockets"
    plugin_dir = plugins / "test-plugin"
    config = tmp_path / "etc" / "lucrium"
    plugin_dir.mkdir(parents=True)
    sockets.mkdir(parents=True)
    config.mkdir(parents=True)

    command = _sandbox_command(
        python="/usr/bin/python3",
        plugin_dir=plugin_dir,
        sockets_dir=sockets,
        data_dir=data,
        config_dir=config,
        network=False,
    )

    assert command[0] == "bwrap"
    assert "--unshare-all" in command
    assert "--share-net" not in command
    assert command[command.index(str(data)) - 1] == "--tmpfs"
    assert command[command.index(str(config)) - 1] == "--tmpfs"
    assert str(plugin_dir) in command
    assert str(sockets) in command
    assert command[-4:] == ["--", "/usr/bin/python3", "-m", "lucrium_plugin_sdk"]


def test_sandbox_command_shares_network_when_permitted(tmp_path: Path) -> None:
    data = tmp_path / "data"
    plugins = data / "plugins"
    sockets = data / "sockets"
    plugin_dir = plugins / "test-plugin"
    plugin_dir.mkdir(parents=True)
    sockets.mkdir(parents=True)

    command = _sandbox_command(
        python="/usr/bin/python3",
        plugin_dir=plugin_dir,
        sockets_dir=sockets,
        data_dir=data,
        config_dir=None,
        network=True,
    )

    assert "--share-net" in command
    assert "--chdir" in command
