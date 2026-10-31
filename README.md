# Lucrium Worker

> **Проект в стадии разработки и тестирования.** Это публичная бета: код открыт,
> ветка `main` обновляется часто, контракты и форматы файлов ещё могут меняться
> без обратной совместимости. Помощь с тестированием, отчёты об ошибках и находки
> безопасности очень нужны — см. [CONTRIBUTING.md](CONTRIBUTING.md). Для реального
> ведения продаж воркер пока **не готов**: не держите на нём рабочие аккаунты
> площадок, для экспериментов используйте тестовые учётные записи.

[![CI](https://github.com/nechel12/lucrium-worker/actions/workflows/ci.yml/badge.svg)](https://github.com/nechel12/lucrium-worker/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.12%2B-blue)](pyproject.toml)

Демон для Linux VPS: держит сессии FunPay / PlayerOK / GGsel через
[`aiosellers`](https://github.com/nechel12/aiosellers), исполняет плагины в
отдельных процессах и отдаёт панели WSS-канал. **Воркер — источник истины**:
панель можно закрыть, он продолжает работать 24/7.

## Требования

- Linux с systemd (Debian/Ubuntu, Fedora, RHEL-подобные); Python 3.12+ ставится
  установщиком через `uv`.
- Открытый наружу порт по умолчанию `8443` — к нему подключается панель.
- `bubblewrap` для изоляции плагинов. На Ubuntu 24.04 неотъемлемые user
  namespaces ограничены AppArmor: если установщик сообщает, что проба bwrap не
  прошла, плагины с `sandbox = "require"` не запустятся, пока не будет создан
  профиль AppArmor для `bwrap` (см. вывод `install.sh`).

## Установка

```bash
curl -fsSL https://raw.githubusercontent.com/nechel12/lucrium-worker/main/install.sh | bash
```

`curl | bash` исполняет чужой код от root — сначала прочитайте скрипт. Скрипт
ставит Python 3.12+, `uv`, systemd-юнит, генерирует pairing-токен и
(по умолчанию) самоподписанный TLS-сертификат, затем печатает `адрес + токен`.
Токен показывается один раз — сохраните его.

Переменные: `LUCRIUM_PORT` (8443), `LUCRIUM_DATA_DIR` (`/var/lib/lucrium`),
`LUCRIUM_TLS=0` (отключить TLS), `LUCRIUM_REF` (тег/коммит, по умолчанию
`main`), `LUCRIUM_REPO_URL`, `LUCRIUM_PROTOCOL_URL`. Значение по умолчанию `main`
устанавливает неподписанный код из ветки развития и печатает предупреждение: для
воспроизводимой установки задавайте подписанный `LUCRIUM_REF=vX.Y.Z`, для `v*`
установщик проверяет SSH-подпись тега. Проверку подписи можно отключить только
осознанно: `LUCRIUM_ALLOW_UNSIGNED_TAGS=1`. Теги, созданные до введения подписи,
не считаются доверенными — используйте ближайший подписанный релиз.

При TLS установщик печатает SHA256 fingerprint самоподписанного сертификата. Его
нужно вставить в панели: проверка TLS включена по умолчанию, а fingerprint даёт
защиту от MITM без отключения проверки.

Установщик также заранее готовит native-библиотеку для `tls_requests` (её
использует `aiosellers`) и прописывает `TLS_LIBRARY_PATH` в systemd: при
`ProtectSystem=strict` и root-owned venv скачивание в `site-packages` при первом
запросе невозможно. Если GitHub с VPS недоступен, скачайте библиотеку вручную и
передайте путь: `LUCRIUM_TLS_LIBRARY=/path/to/tls-client.so bash install.sh`.

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
event_retention_days = 30   # история событий в SQLite

[tls]                       # опционально
certfile = "/etc/lucrium/tls/cert.pem"
keyfile = "/etc/lucrium/tls/key.pem"

[heartbeat]
seconds = 15

[plugins]
memory_limit_mb = 512       # RLIMIT_AS для процесса плагина
sandbox = "require"         # require | auto | off
```

`plugins.sandbox` принимает и строку, и старый булев значение: `true` теперь
означает `require`, `false` — `off`; `auto` оставлен как явный осознанный откат.

Журнал воркера и плагинов дублируется в `data_dir/logs/worker.log` (JSON-строки)
и в journald. Значения полей в ошибках редатируются: `golden_key`, токены и
значения конфигов в лог и в ответы панели не попадают.

## Что внутри

| Модуль | Назначение |
|---|---|
| `config` | TOML-конфиг, генерация токена, каталоги данных |
| `storage` | SQLite (WAL): аккаунты (креды шифруются Fernet at-rest), плагины и снапшот их разрешений, KV плагинов |
| `accounts` | CRUD аккаунтов + capability-матрица площадки |
| `sessions` | по одному клиенту aiosellers на аккаунт, авто-restart с backoff и разносом пауз |
| `plugins.installer` | установка zip-архивом: защита от zip-slip, дублей записей и zip-bomb (счёт по фактическим байтам), валидация `plugin.toml` и JSON-схемы, сверка манифеста после распаковки |
| `plugins.host` | subprocess на плагин, JSON-RPC IPC (unix socket, NDJSON), проверка пира сокета, таймауты, авто-restart |
| `plugins.rpc` | host-сторона JSON-RPC 2.0 |
| `server` | WSS-сервер панели: bearer-токен, backoff по IP, лимиты соединения, envelope-цикл |
| `app` | команды панели, снапшоты состояния, события, heartbeat |
| `export` | экспорт/импорт состояния (`.lcs`, PBKDF2-SHA256 600k + Fernet): аккаунты/прокси, плагины, KV, артефакты плагинов и опционально история |

Протокол и модели — из [`lucrium-protocol`](https://github.com/nechel12/lucrium-protocol);
SDK плагинов — [`lucrium-plugin-sdk`](https://github.com/nechel12/lucrium-protocol/tree/main/packages/lucrium-plugin-sdk).

## Безопасность

Проверка подписанных релизных тегов описана в [SECURITY.md](SECURITY.md).

- Pairing-токен — bearer на WSS-рукопожатие; сравнение в постоянное время.
  Неверные попытки с предъявленным токеном копят backoff по IP; запросы без
  `Authorization` (сканеры порта) отказываются, но не банят адрес — иначе
  легитимная панель за TLS-прокси ловила бы блокировку чужими попытками.
  Живых соединений и входящих кадров тоже ограничено.
- Креды площадок, пароли прокси, конфиги и KV плагинов шифруются at-rest ключом
  `data_dir/secret.key` (0600); каталог данных — `0700`, БД и логи — `0600`.
- `get_state` креды не отдаёт: снапшот состояния содержит статусы, площадки и
  маски. Полные значения возвращает только отдельная команда
  `reveal_credentials`, и каждое раскрытие пишется в журнал воркером как
  предупреждение.
- TLS: самоподписанный сертификат или сертификат домена пользователя. Панель
  проверяет TLS по умолчанию; для self-signed используйте fingerprint.
- **Плагины — чужой код, и это основная граница угрозы.** При наличии
  `bubblewrap` воркер запускает плагин в отдельных namespaces: `--unshare-all`
  без `--share-net`, каталоги данных и конфига скрыты за tmpfs, доступны только
  каталог плагина (read-only) и свой сокет. Сети у плагина нет никакой:
  `network = true` разрешает `ctx.http`, а запрос выполняет воркер со своей
  egress-политикой (запрет loopback, приватных и link-local адресов, проверка
  каждой ступени редиректа).
- Соединение с плагином по unix-сокету принимается только если это наш дочерний
  процесс (проверка `SO_PEERCRED` по pid и uid): сокет доступен любому процессу
  того же uid, поэтому «первым подключился — значит плагин» неприемлемо.
- Разрешения плагина (`[permissions] accounts/network/finance`) фиксируются в
  базе при установке; если файлы на диске просят больше, чем зафиксировано,
  воркер отказывает в запуске, а переустановка или обновление с расширением прав
  требует `ack_permissions` от панели.
- Вызовы клиента через `ctx.session` идут через allowlist корней и классификацию
  опасных методов: платные и необратимые операции (`finance.*`,
  `transactions.*`, `lots.raise_category`, `deals.confirm/complete/create`,
  `items.set_premium_priority`, `games.accept_agreement`, публикация/удаление
  лотов и т.п.) доступны плагину только с `finance = true`. Медианные параметры
  (`image`, `photo`, …) принимаются как id загруженного файла или http(s)-URL:
  локальный путь библиотека прочитала бы с диска воркера, и любой файл VPS ушёл
  бы наружу через чат площадки.
- `sandbox = "require"` (по умолчанию) не даёт запустить плагин без изоляции;
  `auto` оставляет fallback с предупреждением, `off` явно отключает её.
  Устанавливайте только проверенные плагины и считайте, что получивший
  `network`/`finance` плагин умеет всё, что умеет ваша учётная запись.

## Разработка

```bash
git clone https://github.com/nechel12/lucrium-protocol   # должен лежать рядом
git clone https://github.com/nechel12/lucrium-worker
cd lucrium-worker

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
SQLite, экспорт/импорт, `install.sh` + systemd. Не хватает для «production для
чужих рук»: Docker-образ, телеметрия и метрики, переиспытанный на живых
площадках цикл установки/обновления плагинов, удалённая доставка плагинов (v2 —
формат уже заложен в `lucrium-protocol`).

## Лицензия

AGPL-3.0 — см. [LICENSE](LICENSE).
[`aiosellers`](https://github.com/nechel12/aiosellers) — форк Zewsic/aiosellers
под Apache-2.0; уведомления апстрима сохранены в том репозитории.
