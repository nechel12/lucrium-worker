"""Тесты установки плагинов zip-архивом (спека §6.2)."""

from __future__ import annotations

from pathlib import Path

import pytest

from lucrium_worker.plugins.installer import InstallError, PluginInstaller

from .conftest import PLUGIN_SCHEMA, PLUGIN_TOML, make_zip, plugin_zip


def test_install_from_root(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    installed = installer.install(plugin_zip())
    assert installed.manifest.id == "test-plugin"
    assert (installed.directory / "plugin.toml").is_file()
    assert (installed.directory / "plugin_main.py").is_file()


def test_install_from_single_top_level_dir(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    installed = installer.install(plugin_zip(root="test-plugin-main"))
    assert installed.manifest.id == "test-plugin"
    assert (installed.directory / "plugin.toml").is_file()


def test_duplicate_requires_overwrite(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    installer.install(plugin_zip())
    with pytest.raises(InstallError, match="already installed"):
        installer.install(plugin_zip())
    installer.install(plugin_zip(), overwrite=True)


def test_zip_slip_rejected(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    archive = make_zip({"plugin.toml": PLUGIN_TOML, "../evil.txt": "boom"})
    with pytest.raises(InstallError, match="unsafe path"):
        installer.install(archive)
    assert not (tmp_path / "evil.txt").exists()


def test_missing_manifest_rejected(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    with pytest.raises(InstallError, match=r"plugin\.toml"):
        installer.install(make_zip({"main.py": "print('hi')"}))


def test_invalid_manifest_rejected(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    archive = make_zip({"plugin.toml": "[plugin]\nid = 'x'\n"})
    with pytest.raises(InstallError):
        installer.install(archive)


def test_invalid_schema_rejected_and_staging_cleaned(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    archive = make_zip(
        {
            "plugin.toml": PLUGIN_TOML,
            "config_schema.json": "{not json}",
            "plugin_main.py": "",
        }
    )
    with pytest.raises(InstallError):
        installer.install(archive)
    assert not (tmp_path / "plugins" / "test-plugin").exists()
    assert list((tmp_path / "plugins").glob(".staging-*")) == []


def test_bad_zip_rejected(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    with pytest.raises(InstallError, match="not a zip"):
        installer.install(b"definitely not a zip")


def test_schema_present_but_not_declared_is_ignored(tmp_path: Path) -> None:
    """Схема без `[config].schema` не мешает установке — панель просто не покажет форму."""
    installer = PluginInstaller(tmp_path / "plugins")
    manifest = PLUGIN_TOML.replace('[config]\nschema = "config_schema.json"\n', "")
    archive = make_zip({"plugin.toml": manifest, "config_schema.json": PLUGIN_SCHEMA, "plugin_main.py": ""})
    installed = installer.install(archive)
    assert installed.manifest.schema_path is None


def test_remove(tmp_path: Path) -> None:
    installer = PluginInstaller(tmp_path / "plugins")
    installed = installer.install(plugin_zip())
    installer.remove(installed.manifest.id)
    assert not installed.directory.exists()
    installer.remove("missing")  # no-op
