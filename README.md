# Lucrium Worker

Демон для Linux VPS: держит сессии FunPay / PlayerOK / GGsel через `aiosellers`,
исполняет плагины в отдельных процессах и отдаёт панели WSS-канал. **Воркер —
источник истины**: панель можно закрыть, он продолжает работать 24/7.

## Установка

```bash
curl -fsSL https://raw.githubusercontent.com/nechel12/lucrium-worker/main/install.sh | bash
```

Скрипт ставит Python 3.12+, `uv`, systemd-юнит, генерирует pairing-токен и
(по умолчанию) самоподписанный TLS-сертификат, затем печатает `адрес + токен`.
Токен показывается один раз — сохраните его.

Переменные: `LUCRIUM_PORT` (8443), `LUCRIUM_DATA_DIR` (`/var/lib/lucrium`),
`LUCRIUM_TLS=0` (отключить TLS), `LUCRIUM_REPO_URL`, `LUCRIUM_PROTOCOL_URL`.

## CLI

```bash
lucrium-worker init --config /etc/lucrium/worker.toml --data-dir /var/lib/lucrium
lucrium-worker show-token --config /etc/lucrium/worker.toml
lucrium-worker run --config /etc/lucrium/worker.toml
```

`init` создаёт конфиг (0600) и pairing-токен (256 бит). Конфиг:

```toml
[worker]
host = "0.0.0.0"
port = 8443
token = "<64 hex>"

[storage]
data_dir = "/var/lib/lucrium"

[tls]                       # опционально
certfile = "/etc/lucrium/tls/cert.pem"
keyfile = "/etc/lucrium/tls/key.pem"

[heartbeat]
seconds = 15
```

## Что внутри

| Модуль | Назначение |
|---|---|
| `config` | TOML-конфиг, генерация токена, каталоги данных |
| `storage` | SQLite (WAL): аккаунты (креды шифруются Fernet at-rest), плагины, KV плагинов |
| `accounts` | CRUD аккаунтов + capability-матрица площадки |
| `sessions` | по одному клиенту aiosellers на аккаунт, авто-restart с backoff |
| `plugins.installer` | установка zip-архивом: zip-slip/zip-bomb защита, валидация `plugin.toml` и JSON-схемы |
| `plugins.host` | subprocess на плагин, JSON-RPC IPC (unix socket, NDJSON), таймауты, авто-restart |
| `plugins.rpc` | host-сторона JSON-RPC 2.0 |
| `server` | WSS-сервер панели: bearer-токен, rate-limit по IP, envelope-цикл |
| `app` | команды панели, снапшоты состояния, события, heartbeat |
| `export` | экспорт/импорт состояния (`.lcs`, PBKDF2 + Fernet) |

Протокол и модели — из [`lucrium-protocol`](../lucrium-protocol); SDK плагинов —
[`lucrium-plugin-sdk`](../lucrium-protocol/packages/lucrium-plugin-sdk).

## Безопасность

- Pairing-токен — bearer на WSS-рукопожатие; неверные попытки блокируют IP с
  экспоненциальным backoff.
- Креды площадок шифруются at-rest ключом `data_dir/secret.key` (0600).
- `get_state` — единственный ответ с кредами; событие `state` их не содержит.
- Плагин не имеет доступа к сети без `permissions.network = true`; площадки —
  только через `ctx.session`.
- TLS: самоподписанный сертификат или сертификат домена пользователя.

## Разработка

Репозитории должны лежать рядом (`lucrium-worker` и `lucrium-protocol`):

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

Тест `tests/test_plugin_host.py` запускает реальный subprocess плагина через
unix socket — только Linux (на Windows пропускается).

## Статус

v1 (рабочее ядро): WSS-сервер, аккаунты/сессии, host плагинов, установка zip,
SQLite, экспорт/импорт, `install.sh` + systemd. Дальше: Docker-образ,
телеметрия/метрики, удалённая доставка плагинов (v2, формат уже в protocol).

## Лицензия

AGPL-3.0 — см. [LICENSE](LICENSE).
`aiosellers` — форк Zewsic/aiosellers, Apache-2.0; уведомления апстрима сохранены.
