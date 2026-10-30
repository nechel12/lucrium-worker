"""Установка плагинов zip-архивом (спека §6.2, v1).

Архив обязан содержать ``plugin.toml`` либо в корне, либо в единственном
каталоге верхнего уровня (типичный GitHub-zip). Манифест и JSON-схема
валидируются до распаковки в рабочий каталог; zip-slip и zip-bomb отсекаются.
"""

from __future__ import annotations

import shutil
import uuid
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath

from lucrium_plugin_sdk.manifest import ConfigSchemaError, load_config_schema
from lucrium_protocol.manifest import MANIFEST_FILENAME, ManifestError, PluginManifest, parse_manifest_text

MAX_ARCHIVE_BYTES = 48 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 128 * 1024 * 1024
MAX_FILES = 2000


class InstallError(Exception):
    """Архив небезопасен/невалиден или плагин уже установлен."""


@dataclass(frozen=True, slots=True)
class InstalledPlugin:
    manifest: PluginManifest
    directory: Path


class PluginInstaller:
    def __init__(self, plugins_dir: Path) -> None:
        self._plugins_dir = plugins_dir

    @property
    def plugins_dir(self) -> Path:
        return self._plugins_dir

    def directory_for(self, plugin_id: str) -> Path:
        return self._plugins_dir / plugin_id

    def install(self, archive: bytes, *, overwrite: bool = False) -> InstalledPlugin:
        if len(archive) > MAX_ARCHIVE_BYTES:
            raise InstallError(f"archive exceeds {MAX_ARCHIVE_BYTES} bytes")
        try:
            with zipfile.ZipFile(BytesIO(archive)) as zf:
                members = _safe_members(zf)
                root = _find_plugin_root(members)
                manifest = _read_manifest(zf, members, root)
                target = self.directory_for(manifest.id)
                if target.exists() and not overwrite:
                    raise InstallError(f"plugin {manifest.id!r} is already installed (overwrite=false)")
                staging = self._plugins_dir / f".staging-{uuid.uuid4().hex}"
                try:
                    self._extract(zf, members, root, staging)
                    load_config_schema(staging, manifest)
                except (ManifestError, ConfigSchemaError) as exc:
                    shutil.rmtree(staging, ignore_errors=True)
                    raise InstallError(str(exc)) from exc
                except BaseException:
                    shutil.rmtree(staging, ignore_errors=True)
                    raise
                if target.exists():
                    shutil.rmtree(target)
                staging.rename(target)
                return InstalledPlugin(manifest=manifest, directory=target)
        except zipfile.BadZipFile as exc:
            raise InstallError(f"not a zip archive: {exc}") from exc

    def remove(self, plugin_id: str) -> None:
        directory = self.directory_for(plugin_id)
        if directory.exists():
            shutil.rmtree(directory)

    def read_manifest(self, plugin_id: str) -> PluginManifest:
        directory = self.directory_for(plugin_id)
        path = directory / MANIFEST_FILENAME
        if not path.is_file():
            raise InstallError(f"plugin {plugin_id!r} has no {MANIFEST_FILENAME}")
        try:
            return parse_manifest_text(path.read_text(encoding="utf-8"))
        except ManifestError as exc:
            raise InstallError(str(exc)) from exc

    @staticmethod
    def _extract(
        zf: zipfile.ZipFile,
        members: list[zipfile.ZipInfo],
        root: str,
        staging: Path,
    ) -> None:
        staging.mkdir(parents=True, exist_ok=False)
        for info in members:
            relative = _relative_name(info, root)
            if relative is None:
                continue
            destination = staging / relative
            if info.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, destination.open("wb") as dst:
                shutil.copyfileobj(src, dst)


def _safe_members(zf: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    members = zf.infolist()
    if len(members) > MAX_FILES:
        raise InstallError(f"archive has too many files ({len(members)} > {MAX_FILES})")
    total = 0
    for info in members:
        name = info.filename
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or name.startswith("\\"):
            raise InstallError(f"unsafe path in archive: {name!r}")
        if info.is_dir():
            continue
        if (info.external_attr >> 16) & 0o170000 == 0o120000:
            raise InstallError(f"symlinks are not allowed in plugin archives: {name!r}")
        total += info.file_size
        if total > MAX_UNCOMPRESSED_BYTES:
            raise InstallError(f"uncompressed size exceeds {MAX_UNCOMPRESSED_BYTES} bytes")
    return members


def _find_plugin_root(members: list[zipfile.ZipInfo]) -> str:
    names = [m.filename for m in members if not m.is_dir()]
    if MANIFEST_FILENAME in names:
        return ""
    roots = {name.split("/", 1)[0] for name in names if "/" in name}
    if len(roots) == 1:
        root = next(iter(roots))
        if f"{root}/{MANIFEST_FILENAME}" in names:
            return root
    raise InstallError(f"{MANIFEST_FILENAME} must be in the archive root or in a single top-level directory")


def _read_manifest(zf: zipfile.ZipFile, members: list[zipfile.ZipInfo], root: str) -> PluginManifest:
    name = f"{root}/{MANIFEST_FILENAME}" if root else MANIFEST_FILENAME
    info = next((m for m in members if m.filename == name), None)
    if info is None:
        raise InstallError(f"{MANIFEST_FILENAME} not found in archive")
    if info.file_size > 64 * 1024:
        raise InstallError(f"{MANIFEST_FILENAME} is too large")
    try:
        text = zf.read(info).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InstallError(f"{MANIFEST_FILENAME} must be UTF-8: {exc}") from exc
    try:
        return parse_manifest_text(text)
    except ManifestError as exc:
        raise InstallError(f"invalid manifest: {exc}") from exc


def _relative_name(info: zipfile.ZipInfo, root: str) -> Path | None:
    name = info.filename
    if root:
        prefix = f"{root}/"
        if not name.startswith(prefix):
            return None
        name = name[len(prefix) :]
    if not name:
        return None
    return Path(*PurePosixPath(name).parts)


__all__ = [
    "MAX_ARCHIVE_BYTES",
    "MAX_FILES",
    "MAX_UNCOMPRESSED_BYTES",
    "InstallError",
    "InstalledPlugin",
    "PluginInstaller",
]
