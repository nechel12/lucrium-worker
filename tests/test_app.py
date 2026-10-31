"""Тесты ядра воркера: команды панели, состояние, плагины без процессов."""

from __future__ import annotations

import base64
from typing import Any

from lucrium_protocol import (
    AddAccountParams,
    CallClientParams,
    Command,
    ErrorCode,
    GetLogsParams,
    GetPluginSchemaParams,
    GetStateParams,
    ImportMode,
    ImportStateParams,
    InstallPluginParams,
    LogLevel,
    LogScope,
    PingParams,
    RemoveAccountParams,
    Response,
    SessionStatus,
    SetPluginConfigParams,
    StateWithCredentials,
    StopSessionParams,
    UpdateAccountParams,
    UpdatePluginParams,
    build_request,
    decode_result,
    parse_envelope,
)
from pydantic import SecretStr

from lucrium_worker.app import WorkerApp

from .conftest import PLUGIN_MAIN, PLUGIN_SCHEMA, PLUGIN_TOML, make_zip, plugin_zip


async def _call(app: WorkerApp, command: Command, params: Any) -> Response:
    response = await app.handle_request(build_request(command, params))
    parsed = parse_envelope(response.model_dump_json())
    assert isinstance(parsed, Response)
    return parsed


def _result(app: WorkerApp, response: Response, command: Command) -> Any:
    assert response.payload.error is None, response.payload.error
    return decode_result(command, response.payload.result or {})


async def test_ping(app: WorkerApp) -> None:
    response = await _call(app, Command.PING, PingParams(nonce="abc"))
    result = _result(app, response, Command.PING)
    assert result.nonce == "abc"
    assert result.revision >= 0


async def test_add_account_and_get_state_with_credentials(app: WorkerApp) -> None:
    params = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "funpay",
                "label": "Основной",
                "credentials": {"platform": "funpay", "golden_key": "gk-live"},
            }
        }
    )
    response = await _call(app, Command.ADD_ACCOUNT, params)
    ref = _result(app, response, Command.ADD_ACCOUNT)
    assert ref.account_id.startswith("acc-")
    assert app.sessions.started == [ref.account_id]  # type: ignore[attr-defined]

    state = _result(app, await _call(app, Command.GET_STATE, GetStateParams()), Command.GET_STATE)
    assert isinstance(state, StateWithCredentials)
    assert state.accounts[0].account.credentials.secret_value() == "gk-live"

    snapshot = await app.state_snapshot()
    assert snapshot.accounts[0].capabilities  # FunPay: messages/orders/...
    assert "gk-live" not in snapshot.model_dump_json()


async def test_remove_account_stops_session(app: WorkerApp) -> None:
    params = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "ggsel",
                "label": "GGsel",
                "credentials": {"platform": "ggsel", "api_key": "key", "seller_id": "1"},
            }
        }
    )
    ref = _result(app, await _call(app, Command.ADD_ACCOUNT, params), Command.ADD_ACCOUNT)
    removed = RemoveAccountParams(account_id=ref.account_id)
    _result(app, await _call(app, Command.REMOVE_ACCOUNT, removed), Command.REMOVE_ACCOUNT)
    assert ref.account_id in app.sessions.stopped  # type: ignore[attr-defined]
    assert await app.storage.get_account(ref.account_id) is None


async def test_install_plugin_without_start(app: WorkerApp) -> None:
    params = InstallPluginParams(archive_b64=base64.b64encode(plugin_zip()).decode(), auto_start=False)
    state = _result(app, await _call(app, Command.INSTALL_PLUGIN, params), Command.INSTALL_PLUGIN)
    assert state.id == "test-plugin"
    assert state.has_config_schema is True
    assert state.status.value == "stopped"
    assert (app.config.plugins_dir / "test-plugin" / "plugin.toml").is_file()


async def test_set_plugin_config_validates_schema(app: WorkerApp) -> None:
    install = InstallPluginParams(archive_b64=base64.b64encode(plugin_zip()).decode(), auto_start=False)
    _result(app, await _call(app, Command.INSTALL_PLUGIN, install), Command.INSTALL_PLUGIN)

    bad = SetPluginConfigParams(plugin_id="test-plugin", config={"greeting": ""})
    response = await _call(app, Command.SET_PLUGIN_CONFIG, bad)
    assert response.payload.error is not None
    assert response.payload.error.code is ErrorCode.INVALID_PARAMS

    good = SetPluginConfigParams(plugin_id="test-plugin", config={"greeting": "Привет"})
    result = _result(app, await _call(app, Command.SET_PLUGIN_CONFIG, good), Command.SET_PLUGIN_CONFIG)
    assert result.restart_triggered is False


async def test_get_plugin_schema(app: WorkerApp) -> None:

    install = InstallPluginParams(archive_b64=base64.b64encode(plugin_zip()).decode(), auto_start=False)
    _result(app, await _call(app, Command.INSTALL_PLUGIN, install), Command.INSTALL_PLUGIN)
    result = _result(
        app,
        await _call(app, Command.GET_PLUGIN_SCHEMA, GetPluginSchemaParams(plugin_id="test-plugin")),
        Command.GET_PLUGIN_SCHEMA,
    )
    assert result.plugin_id == "test-plugin"
    assert result.config_schema["properties"]["greeting"]["type"] == "string"
    assert result.config == {}


async def test_get_events_history(app: WorkerApp) -> None:
    from datetime import UTC, datetime

    from lucrium_protocol import EventKind, GetEventsParams, HubEvent, Platform

    await app.storage.add_event(
        HubEvent(
            platform=Platform.FUNPAY,
            session_id="acc-1",
            kind=EventKind.MESSAGE,
            payload={"message": {"text": "hi"}},
            ts=datetime.now(UTC),
        )
    )
    result = _result(app, await _call(app, Command.GET_EVENTS, GetEventsParams()), Command.GET_EVENTS)
    assert len(result.events) == 1
    assert result.events[0].payload["message"]["text"] == "hi"


async def test_call_client(app: WorkerApp) -> None:
    add = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "funpay",
                "label": "FunPay",
                "credentials": {"platform": "funpay", "golden_key": "gk"},
            }
        }
    )
    ref = _result(app, await _call(app, Command.ADD_ACCOUNT, add), Command.ADD_ACCOUNT)
    params = CallClientParams(session_id=ref.account_id, client_method="lots.list_self", params={"subcategory_id": 1})
    result = _result(app, await _call(app, Command.CALL_CLIENT, params), Command.CALL_CLIENT)
    assert result.data == {"ok": True, "method": "lots.list_self"}
    assert app.sessions.calls[-1] == (ref.account_id, "lots.list_self", {"subcategory_id": 1})  # type: ignore[attr-defined]


async def test_call_client_rejects_financial_and_non_panel_methods(app: WorkerApp) -> None:
    add = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "funpay",
                "label": "FunPay",
                "credentials": {"platform": "funpay", "golden_key": "gk"},
            }
        }
    )
    ref = _result(app, await _call(app, Command.ADD_ACCOUNT, add), Command.ADD_ACCOUNT)
    for method in ("finance.withdraw", "transactions.withdraw", "orders.refund", "chats.send_message"):
        response = await _call(
            app,
            Command.CALL_CLIENT,
            CallClientParams(session_id=ref.account_id, client_method=method),
        )
        assert response.payload.error is not None
        assert response.payload.error.code is ErrorCode.FORBIDDEN
    assert app.sessions.calls == []  # type: ignore[attr-defined]


async def test_get_logs(app: WorkerApp) -> None:
    from lucrium_protocol import LogLevel

    app.logs.add(LogLevel.INFO, "worker", "worker line")
    app.logs.add(LogLevel.ERROR, "test-plugin", "plugin line")
    result = _result(app, await _call(app, Command.GET_LOGS, GetLogsParams(scope=LogScope.WORKER)), Command.GET_LOGS)
    messages = [entry.message for entry in result.entries]
    assert "worker line" in messages
    assert "plugin line" not in messages
    result = _result(
        app,
        await _call(app, Command.GET_LOGS, GetLogsParams(scope=LogScope.PLUGIN, plugin_id="test-plugin")),
        Command.GET_LOGS,
    )
    assert [entry.message for entry in result.entries] == ["plugin line"]


async def test_rotate_token(app: WorkerApp) -> None:
    from lucrium_protocol import RotateTokenParams

    old = app.token
    result = _result(app, await _call(app, Command.ROTATE_TOKEN, RotateTokenParams()), Command.ROTATE_TOKEN)
    assert result.token != old
    assert app.token == result.token


async def test_export_import_via_commands(app: WorkerApp) -> None:
    add = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "playerok",
                "label": "PlayerOK",
                "credentials": {"platform": "playerok", "access_token": "po-token"},
            }
        }
    )
    _result(app, await _call(app, Command.ADD_ACCOUNT, add), Command.ADD_ACCOUNT)

    export = await app.handle_request(build_request(Command.EXPORT_STATE, _export_params("passphrase-123")))
    exported = _result(app, export, Command.EXPORT_STATE)
    assert exported.size_bytes > 0

    await app.storage.clear_all()
    imported = _result(
        app,
        await _call(
            app,
            Command.IMPORT_STATE,
            ImportStateParams(
                blob_b64=exported.blob_b64,
                passphrase=SecretStr("passphrase-123"),
                mode=ImportMode.REPLACE,
            ),
        ),
        Command.IMPORT_STATE,
    )
    assert imported.accounts == 1
    assert imported.sessions_started == 1
    accounts = await app.storage.list_accounts()
    assert len(accounts) == 1
    assert accounts[0].platform.value == "playerok"
    assert accounts[0].credentials.secret_value() == "po-token"


async def test_wrong_passphrase_does_not_stop_sessions(app: WorkerApp) -> None:
    add = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "funpay",
                "label": "FunPay",
                "credentials": {"platform": "funpay", "golden_key": "gk"},
            }
        }
    )
    _result(app, await _call(app, Command.ADD_ACCOUNT, add), Command.ADD_ACCOUNT)
    export = _result(
        app,
        await app.handle_request(build_request(Command.EXPORT_STATE, _export_params("passphrase-123"))),
        Command.EXPORT_STATE,
    )
    app.sessions.stopped.clear()  # type: ignore[attr-defined]

    response = await _call(
        app,
        Command.IMPORT_STATE,
        ImportStateParams(
            blob_b64=export.blob_b64,
            passphrase=SecretStr("wrong-passphrase"),
            mode=ImportMode.MERGE,
        ),
    )
    assert response.payload.error is not None
    assert response.payload.error.code is ErrorCode.INVALID_PARAMS
    assert app.sessions.stopped == []  # type: ignore[attr-defined]


async def test_stop_session_persists_desired_running(app: WorkerApp) -> None:
    add = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "funpay",
                "label": "FunPay",
                "credentials": {"platform": "funpay", "golden_key": "gk"},
            }
        }
    )
    ref = _result(app, await _call(app, Command.ADD_ACCOUNT, add), Command.ADD_ACCOUNT)
    _result(
        app,
        await _call(app, Command.STOP_SESSION, StopSessionParams(account_id=ref.account_id)),
        Command.STOP_SESSION,
    )
    record = await app.storage.get_account(ref.account_id)
    assert record is not None and record.desired_running is False


async def test_update_account_restarts_error_session(app: WorkerApp) -> None:
    add = AddAccountParams.model_validate(
        {
            "account": {
                "platform": "funpay",
                "label": "FunPay",
                "credentials": {"platform": "funpay", "golden_key": "gk"},
            }
        }
    )
    ref = _result(app, await _call(app, Command.ADD_ACCOUNT, add), Command.ADD_ACCOUNT)
    app.sessions.statuses[ref.account_id] = SessionStatus.ERROR  # type: ignore[attr-defined]
    started_before = len(app.sessions.started)  # type: ignore[attr-defined]

    _result(
        app,
        await _call(
            app,
            Command.UPDATE_ACCOUNT,
            UpdateAccountParams(account_id=ref.account_id, label="Новое имя"),
        ),
        Command.UPDATE_ACCOUNT,
    )
    assert len(app.sessions.started) == started_before + 1  # type: ignore[attr-defined]
    assert ref.account_id in app.sessions.stopped  # type: ignore[attr-defined]


async def test_get_logs_returns_latest_entries(app: WorkerApp) -> None:
    for index in range(300):
        app.logs.add(LogLevel.INFO, "worker", f"line {index}")
    result = _result(
        app,
        await _call(app, Command.GET_LOGS, GetLogsParams(scope=LogScope.WORKER, limit=200)),
        Command.GET_LOGS,
    )
    assert result.entries[0].message == "line 100"
    assert result.entries[-1].message == "line 299"


async def test_update_plugin_with_wrong_id_does_not_touch_existing(app: WorkerApp) -> None:
    install = InstallPluginParams(archive_b64=base64.b64encode(plugin_zip()).decode(), auto_start=False)
    _result(app, await _call(app, Command.INSTALL_PLUGIN, install), Command.INSTALL_PLUGIN)
    other = PLUGIN_TOML.replace('id = "test-plugin"', 'id = "other-plugin"')
    archive = make_zip({"plugin.toml": other, "config_schema.json": PLUGIN_SCHEMA, "plugin_main.py": PLUGIN_MAIN})
    response = await _call(
        app,
        Command.UPDATE_PLUGIN,
        UpdatePluginParams(
            plugin_id="test-plugin",
            archive_b64=base64.b64encode(archive).decode(),
            auto_restart=False,
        ),
    )
    assert response.payload.error is not None
    assert (app.config.plugins_dir / "test-plugin" / "plugin.toml").is_file()
    assert not (app.config.plugins_dir / "other-plugin").exists()


def _export_params(passphrase: str) -> Any:
    from lucrium_protocol import ExportStateParams

    return ExportStateParams(passphrase=SecretStr(passphrase))
