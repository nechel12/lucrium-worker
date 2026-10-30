"""CLI воркера: ``init`` (онбординг), ``run`` (демон), ``show-token``.

lucrium-worker init --data-dir /var/lib/lucrium --port 8443
lucrium-worker run --config /etc/lucrium/worker.toml
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import socket
import sys
from pathlib import Path

from . import __version__
from .app import WorkerApp
from .config import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_DIR,
    ConfigError,
    TlsConfig,
    WorkerConfig,
    create_config,
    ensure_directories,
)
from .server import PanelServer


def main(argv: list[str] | None = None) -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="путь к worker.toml")

    parser = argparse.ArgumentParser(
        prog="lucrium-worker", description="Lucrium Worker — демон панели", parents=[common]
    )
    parser.add_argument("--version", action="version", version=f"lucrium-worker {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    init = subparsers.add_parser("init", help="создать конфиг и pairing-токен", parents=[common])
    init.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    init.add_argument("--host", default="0.0.0.0")
    init.add_argument("--port", type=int, default=8443)
    init.add_argument("--tls-cert", default=None)
    init.add_argument("--tls-key", default=None)
    init.add_argument("--force", action="store_true", help="перезаписать существующий конфиг")

    subparsers.add_parser("show-token", help="показать текущий pairing-токен", parents=[common])
    subparsers.add_parser("run", help="запустить демон (по умолчанию)", parents=[common])

    args = parser.parse_args(argv)
    command = args.command or "run"
    try:
        if command == "init":
            return _init(args)
        if command == "show-token":
            return _show_token(Path(args.config))
        return _run(Path(args.config))
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


def _init(args: argparse.Namespace) -> int:
    path = Path(args.config)
    if path.exists() and not args.force:
        print(f"error: config already exists: {path} (use --force to overwrite)", file=sys.stderr)
        return 2
    tls = None
    if args.tls_cert or args.tls_key:
        if not (args.tls_cert and args.tls_key):
            print("error: --tls-cert and --tls-key must be passed together", file=sys.stderr)
            return 2
        tls = TlsConfig(certfile=Path(args.tls_cert), keyfile=Path(args.tls_key))
    config = create_config(data_dir=Path(args.data_dir), host=args.host, port=args.port, tls=tls)
    ensure_directories(config)
    config.save(path)
    address = _public_address(config.host)
    print("Lucrium Worker готов.")
    print(f"  Конфиг: {path}")
    print(f"  Адрес:  {address}:{config.port}")
    print(f"  Токен:  {config.token}")
    print()
    print("Введите адрес и токен в панели. Токен показывается один раз — сохраните его.")
    return 0


def _show_token(path: Path) -> int:
    config = WorkerConfig.load(path)
    print(config.token)
    return 0


def _run(path: Path) -> int:
    config = WorkerConfig.load(path)
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_serve(config, path))
    except KeyboardInterrupt:
        return 130
    return 0


async def _serve(config: WorkerConfig, path: Path) -> None:
    app = WorkerApp(config, config_path=path)
    server = PanelServer(config, app, app.logs)
    await app.start()
    await server.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop_event.set)

    shutdown = asyncio.create_task(app.wait_closed(), name="worker-shutdown")
    interrupted = asyncio.create_task(stop_event.wait(), name="worker-signal")
    try:
        await asyncio.wait({shutdown, interrupted}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (shutdown, interrupted):
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(shutdown, interrupted, return_exceptions=True)
        await server.stop()
        await app.stop()


def _public_address(host: str) -> str:
    if host not in {"0.0.0.0", "::", ""}:
        return host
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "SERVER_IP"


if __name__ == "__main__":
    sys.exit(main())
