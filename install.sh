#!/usr/bin/env bash
# Lucrium Worker installer (Linux VPS).
#
#   curl -fsSL https://raw.githubusercontent.com/nechel12/lucrium-worker/main/install.sh | bash
#
# Переменные окружения:
#   LUCRIUM_PORT=8443            порт WSS
#   LUCRIUM_INSTALL_DIR=/opt/lucrium
#   LUCRIUM_CONFIG_DIR=/etc/lucrium
#   LUCRIUM_DATA_DIR=/var/lib/lucrium
#   LUCRIUM_USER=lucrium
#   LUCRIUM_TLS=1                сгенерировать самоподписанный сертификат
#   LUCRIUM_REPO_URL, LUCRIUM_PROTOCOL_URL — альтернативные git-адреса
set -euo pipefail

PORT="${LUCRIUM_PORT:-8443}"
INSTALL_DIR="${LUCRIUM_INSTALL_DIR:-/opt/lucrium}"
CONFIG_DIR="${LUCRIUM_CONFIG_DIR:-/etc/lucrium}"
DATA_DIR="${LUCRIUM_DATA_DIR:-/var/lib/lucrium}"
SERVICE_USER="${LUCRIUM_USER:-lucrium}"
TLS_ENABLED="${LUCRIUM_TLS:-1}"
PYTHON_VERSION="${LUCRIUM_PYTHON:-3.12}"
WORKER_URL="${LUCRIUM_REPO_URL:-https://github.com/nechel12/lucrium-worker.git}"
PROTOCOL_URL="${LUCRIUM_PROTOCOL_URL:-https://github.com/nechel12/lucrium-protocol.git}"
# Для воспроизводимой установки укажите тег/коммит: LUCRIUM_REF=v1.2.3
REF="${LUCRIUM_REF:-main}"
CONFIG_FILE="$CONFIG_DIR/worker.toml"

log() { printf '\033[1;34m[lucrium]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[lucrium] error:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run as root (sudo bash install.sh)"
command -v systemctl >/dev/null || die "systemd is required"
command -v git >/dev/null || die "git is required"

install_packages() {
  if command -v apt-get >/dev/null; then
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 python3-venv python3-pip curl ca-certificates openssl
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq bubblewrap || log "bubblewrap unavailable; plugin sandbox disabled"
  elif command -v dnf >/dev/null; then
    dnf install -y -q python3 python3-pip curl ca-certificates openssl
    dnf install -y -q bubblewrap || log "bubblewrap unavailable; plugin sandbox disabled"
  elif command -v yum >/dev/null; then
    yum install -y -q python3 python3-pip curl ca-certificates openssl
    yum install -y -q bubblewrap || log "bubblewrap unavailable; plugin sandbox disabled"
  else
    log "unknown package manager; assuming curl/openssl are installed"
  fi
}

install_packages

# Публичный адрес нужен и для SAN сертификата, и для итогового вывода.
IP="$(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')"
[ -n "$IP" ] || IP="SERVER_IP"

if ! command -v uv >/dev/null; then
  log "installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh >/dev/null
fi
command -v uv >/dev/null || die "uv installation failed"

log "installing Python $PYTHON_VERSION (uv-managed)"
uv python install "$PYTHON_VERSION" >/dev/null || die "cannot install Python $PYTHON_VERSION"

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  log "creating service user $SERVICE_USER"
  useradd --system --home "$INSTALL_DIR" --shell /usr/sbin/nologin "$SERVICE_USER" 2>/dev/null \
    || useradd --system --home "$INSTALL_DIR" --shell /sbin/nologin "$SERVICE_USER"
fi

mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$DATA_DIR"

clone_or_update() {
  local url="$1" target="$2"
  if [ -d "$target/.git" ]; then
    log "updating $(basename "$target") to $REF"
    git -C "$target" fetch --quiet --depth 1 origin "$REF"
    git -C "$target" reset --hard --quiet FETCH_HEAD
  else
    log "cloning $(basename "$target") @ $REF"
    git clone --quiet --depth 1 --branch "$REF" "$url" "$target"
  fi
}

clone_or_update "$PROTOCOL_URL" "$INSTALL_DIR/lucrium-protocol"
clone_or_update "$WORKER_URL" "$INSTALL_DIR/lucrium-worker"

log "syncing python environment"
(cd "$INSTALL_DIR/lucrium-worker" && uv sync --quiet --frozen --python "$PYTHON_VERSION")

WORKER_BIN="$INSTALL_DIR/lucrium-worker/.venv/bin/lucrium-worker"
[ -x "$WORKER_BIN" ] || die "worker entrypoint not found: $WORKER_BIN"

FINGERPRINT=""
FRESH=0
if [ ! -f "$CONFIG_FILE" ]; then
  FRESH=1
  TLS_ARGS=()
  if [ "$TLS_ENABLED" = "1" ] && command -v openssl >/dev/null; then
    log "generating self-signed TLS certificate"
    mkdir -p "$CONFIG_DIR/tls"
    SAN="DNS:localhost,IP:127.0.0.1"
    if [ "$IP" != "SERVER_IP" ]; then
      if printf '%s' "$IP" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$|^[0-9a-fA-F:]+$'; then
        SAN="$SAN,IP:$IP"
      else
        SAN="$SAN,DNS:$IP"
      fi
    fi
    # -addext есть не во всех старых OpenSSL; без SAN сертификат всё равно
    # можно закрепить через fingerprint.
    if ! openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=lucrium" \
      -addext "subjectAltName=$SAN" \
      -keyout "$CONFIG_DIR/tls/key.pem" -out "$CONFIG_DIR/tls/cert.pem" >/dev/null 2>&1; then
      openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=lucrium" \
        -keyout "$CONFIG_DIR/tls/key.pem" -out "$CONFIG_DIR/tls/cert.pem" >/dev/null 2>&1
    fi
    chmod 600 "$CONFIG_DIR/tls/key.pem"
    FINGERPRINT="$(openssl x509 -in "$CONFIG_DIR/tls/cert.pem" -noout -fingerprint -sha256 \
      | sed 's/^.*=//; s/://g' | tr 'A-F' 'a-f')"
    TLS_ARGS=(--tls-cert "$CONFIG_DIR/tls/cert.pem" --tls-key "$CONFIG_DIR/tls/key.pem")
  fi
  log "generating config and pairing token"
  "$WORKER_BIN" init --config "$CONFIG_FILE" --data-dir "$DATA_DIR" --port "$PORT" "${TLS_ARGS[@]}" >/dev/null
fi

if [ -z "$FINGERPRINT" ] && [ -f "$CONFIG_DIR/tls/cert.pem" ]; then
  FINGERPRINT="$(openssl x509 -in "$CONFIG_DIR/tls/cert.pem" -noout -fingerprint -sha256 \
    | sed 's/^.*=//; s/://g' | tr 'A-F' 'a-f')"
fi

chown -R "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR"
chmod 700 "$DATA_DIR"
chown "$SERVICE_USER:$SERVICE_USER" "$CONFIG_FILE"
chmod 600 "$CONFIG_FILE"
if [ -d "$CONFIG_DIR/tls" ]; then
  chown -R "$SERVICE_USER:$SERVICE_USER" "$CONFIG_DIR/tls"
  chmod 600 "$CONFIG_DIR/tls/key.pem" "$CONFIG_DIR/tls/cert.pem"
fi

log "installing systemd unit"
cat >/etc/systemd/system/lucrium-worker.service <<EOF
[Unit]
Description=Lucrium Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory="$INSTALL_DIR/lucrium-worker"
ExecStart="$WORKER_BIN" run --config "$CONFIG_FILE"
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths="$DATA_DIR" "$CONFIG_DIR"
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable lucrium-worker.service >/dev/null
systemctl restart lucrium-worker.service

TOKEN="$("$WORKER_BIN" show-token --config "$CONFIG_FILE")"
SCHEME="ws"; [ "$TLS_ENABLED" = "1" ] && [ -f "$CONFIG_DIR/tls/cert.pem" ] && SCHEME="wss"

echo
echo "============================================================"
echo " Lucrium Worker установлен и запущен"
echo "   Адрес: $SCHEME://$IP:$PORT"
if [ "$FRESH" = "1" ]; then
  echo "   Токен: $TOKEN"
else
  echo "   Токен: не менялся (показать: $WORKER_BIN show-token --config $CONFIG_FILE)"
fi
if [ -n "$FINGERPRINT" ] && [ "$SCHEME" = "wss" ]; then
  echo "   Fingerprint сертификата (SHA256):"
  echo "   $FINGERPRINT"
fi
echo "============================================================"
if [ "$FRESH" = "1" ]; then
  echo " Сохраните токен и fingerprint: для self-signed сертификата"
  echo " fingerprint защищает панель от MITM."
else
  echo " Конфиг уже существовал — токен не менялся."
fi
echo " Статус: systemctl status lucrium-worker"
echo " Логи:   journalctl -u lucrium-worker -f"
echo
