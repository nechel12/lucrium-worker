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


def test_event_retention_default_and_override(tmp_path: Path) -> None:
    config = create_config(data_dir=tmp_path / "data")
    assert config.event_retention_days == 30
    path = tmp_path / "worker.toml"
    text = config.save(path).read_text(encoding="utf-8")
    text = text.replace("event_retention_days = 30", "event_retention_days = 7")
    path.write_text(text, encoding="utf-8")
    assert WorkerConfig.load(path).event_retention_days == 7


def test_plugin_memory_limit_default_and_override(tmp_path: Path) -> None:
    config = create_config(data_dir=tmp_path / "data")
    assert config.plugin_memory_limit_mb == 512
    path = tmp_path / "worker.toml"
    text = config.save(path).read_text(encoding="utf-8")
    text = text.replace("memory_limit_mb = 512", "memory_limit_mb = 256")
    path.write_text(text, encoding="utf-8")
    assert WorkerConfig.load(path).plugin_memory_limit_mb == 256


def test_invalid_token_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        WorkerConfig(data_dir=tmp_path, token="short")


def test_invalid_port_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        WorkerConfig(data_dir=tmp_path, token="a" * 64, port=70000)


def test_non_hex_token_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        WorkerConfig(data_dir=tmp_path, token="z" * 64)


def test_non_finite_heartbeat_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        WorkerConfig(data_dir=tmp_path, token="a" * 64, heartbeat_seconds=float("nan"))


def test_cli_config_before_subcommand_is_used(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from lucrium_worker.__main__ import main

    config = create_config(data_dir=tmp_path / "data", host="127.0.0.1", port=9443)
    path = config.save(tmp_path / "custom.toml")
    assert main(["--config", str(path), "show-token"]) == 0
    assert config.token in capsys.readouterr().out


@pytest.mark.parametrize(
    ("raw", "enabled", "required"),
    [
        ("require", True, True),
        ("auto", True, False),
        ("off", False, False),
        (True, True, True),
        (False, False, False),
    ],
)
def test_plugin_sandbox_modes(tmp_path: Path, raw: object, enabled: bool, required: bool) -> None:
    path = tmp_path / "worker.toml"
    value = f'"{raw}"' if isinstance(raw, str) else str(raw).lower()
    token_line = "a" * 64
    path.write_text(
        f'[worker]\ntoken = "{token_line}"\n[plugins]\nsandbox = {value}\n',
        encoding="utf-8",
    )
    loaded = WorkerConfig.load(path)
    assert loaded.plugin_sandbox is enabled
    assert loaded.plugin_sandbox_required is required


def test_tls_table_must_be_a_table(tmp_path: Path) -> None:
    path = tmp_path / "worker.toml"
    path.write_text('tls = "not a table"\n[worker]\ntoken = "' + "a" * 64 + '"\n', encoding="utf-8")
    with pytest.raises(ConfigError, match="tls"):
        WorkerConfig.load(path)


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
