"""Тесты конфигурации и pairing-токена (спека §3.1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from lucrium_worker.config import (
    ConfigError,
    TlsConfig,
    WorkerConfig,
    create_config,
    generate_token,
)


def test_generate_token_is_256_bit_hex() -> None:
    token = generate_token()
    assert len(token) == 64
    int(token, 16)
    assert token != generate_token()


def test_save_load_roundtrip(tmp_path: Path) -> None:
    config = create_config(data_dir=tmp_path / "data", host="127.0.0.1", port=9443)
    path = config.save(tmp_path / "worker.toml")
    loaded = WorkerConfig.load(path)
    assert loaded.token == config.token
    assert loaded.port == 9443
    assert loaded.host == "127.0.0.1"
    assert loaded.data_dir == tmp_path / "data"
    assert loaded.tls is None


def test_tls_roundtrip(tmp_path: Path) -> None:
    tls = TlsConfig(certfile=tmp_path / "cert.pem", keyfile=tmp_path / "key.pem")
    config = create_config(data_dir=tmp_path / "data", tls=tls)
    loaded = WorkerConfig.load(config.save(tmp_path / "worker.toml"))
    assert loaded.tls == tls


def test_invalid_token_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        WorkerConfig(data_dir=tmp_path, token="short")


def test_invalid_port_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        WorkerConfig(data_dir=tmp_path, token="a" * 64, port=70000)


def test_missing_config(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        WorkerConfig.load(tmp_path / "missing.toml")


def test_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = create_config(data_dir=tmp_path / "data")
    path = config.save(tmp_path / "worker.toml")
    monkeypatch.setenv("LUCRIUM_DATA_DIR", str(tmp_path / "other"))
    monkeypatch.setenv("LUCRIUM_TOKEN", "b" * 64)
    loaded = WorkerConfig.load(path)
    assert loaded.data_dir == tmp_path / "other"
    assert loaded.token == "b" * 64


def test_invalid_toml(tmp_path: Path) -> None:
    path = tmp_path / "bad.toml"
    path.write_text("this is not toml", encoding="utf-8")
    with pytest.raises(ConfigError, match="invalid TOML"):
        WorkerConfig.load(path)
