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
#   LUCRIUM_TLS_LIBRARY=/path/to/libtls-client.so
#                                готовый native tls_requests library (иначе скачается)
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
# Для воспроизводимой установки укажите подписанный тег: LUCRIUM_REF=v1.2.3
REF="${LUCRIUM_REF:-main}"
RELEASE_SIGNING_PUBKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILBugtb8eBzKbNrkbJ1GH9h0NPIGtm3LTODxg6iprSGW lucrium-release"
CONFIG_FILE="$CONFIG_DIR/worker.toml"
PYTHON_DIR="$INSTALL_DIR/python"
TLS_DIR="$INSTALL_DIR/tls"
# Python и native TLS library должны быть доступны пользователю службы:
# /root для него закрыт (и ProtectHome), а /opt читается под ProtectSystem=strict.
export UV_PYTHON_INSTALL_DIR="$PYTHON_DIR"

log() { printf '\033[1;34m[lucrium]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[lucrium] error:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run as root (sudo bash install.sh)"
command -v systemctl >/dev/null || die "systemd is required"

install_packages() {
  if command -v apt-get >/dev/null; then
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git python3 python3-venv python3-pip curl ca-certificates openssl
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq bubblewrap || log "bubblewrap unavailable; plugin sandbox disabled"
  elif command -v dnf >/dev/null; then
    dnf install -y -q git python3 python3-pip curl ca-certificates openssl
    dnf install -y -q bubblewrap || log "bubblewrap unavailable; plugin sandbox disabled"
  elif command -v yum >/dev/null; then
    yum install -y -q git python3 python3-pip curl ca-certificates openssl
    yum install -y -q bubblewrap || log "bubblewrap unavailable; plugin sandbox disabled"
  else
    log "unknown package manager; assuming git/curl/openssl are installed"
  fi
}

install_packages
command -v git >/dev/null || die "git is required"
command -v openssl >/dev/null || log "warning: openssl is missing; TLS certificate will not be generated"

# Публичный адрес нужен и для SAN сертификата, и для итогового вывода.
IP="$(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')"
[ -n "$IP" ] || IP="SERVER_IP"

if command -v bwrap >/dev/null; then
  if ! bwrap --unshare-all --cap-drop ALL --ro-bind / / --dev /dev --proc /proc -- true >/dev/null 2>&1; then
    log "warning: bubblewrap probe failed; plugins with sandbox=require will not start"
  fi
else
  log "warning: bubblewrap is not installed; plugins with sandbox=require will not start"
fi

if ! command -v uv >/dev/null; then
  log "installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh >/dev/null
fi
command -v uv >/dev/null || die "uv installation failed"

mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$DATA_DIR"
case "$CONFIG_DIR" in
  "/"|"/etc"|"/usr"|"/var"|"/home"|"/root"|"/opt"|"/bin"|"/sbin")
    die "LUCRIUM_CONFIG_DIR must be a dedicated directory, got $CONFIG_DIR"
    ;;
esac

log "installing Python $PYTHON_VERSION (uv-managed)"
uv python install --no-bin "$PYTHON_VERSION" >/dev/null || die "cannot install Python $PYTHON_VERSION"

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  log "creating service user $SERVICE_USER"
  useradd --system --home "$INSTALL_DIR" --shell /usr/sbin/nologin "$SERVICE_USER" 2>/dev/null \
    || useradd --system --home "$INSTALL_DIR" --shell /sbin/nologin "$SERVICE_USER"
fi

# Панель ротирует токен через config.save(): службе нужен write на каталог
# конфига, а не только на сам worker.toml.
chown "$SERVICE_USER:$SERVICE_USER" "$CONFIG_DIR"
chmod 700 "$CONFIG_DIR"

verify_release_tag() {
  local target="$1" ref="$2" allowed
  if [ "${LUCRIUM_ALLOW_UNSIGNED_TAGS:-0}" = "1" ]; then
    log "warning: signature verification disabled by LUCRIUM_ALLOW_UNSIGNED_TAGS=1"
    return 0
  fi
  allowed="$(mktemp)"
  printf 'lucrium-release %s\n' "$RELEASE_SIGNING_PUBKEY" >"$allowed"
  if ! git -C "$target" -c gpg.ssh.allowedSignersFile="$allowed" verify-tag "$ref" >/dev/null 2>&1; then
    rm -f "$allowed"
    die "release tag $ref has no valid SSH signature from the Lucrium release key"
  fi
  rm -f "$allowed"
  log "verified release tag $ref"
}

clone_or_update() {
  local url="$1" target="$2"
  if [ -d "$target/.git" ]; then
    log "updating $(basename "$target") to $REF"
    if printf '%s' "$REF" | grep -Eq '^v[0-9]'; then
      git -C "$target" fetch --quiet --depth 1 origin "refs/tags/$REF:refs/tags/$REF"
      verify_release_tag "$target" "$REF"
      git -C "$target" reset --hard --quiet "refs/tags/$REF^{commit}"
    else
      log "warning: $REF is not a release tag; installing an unverified development ref"
      git -C "$target" fetch --quiet --depth 1 origin "$REF"
      git -C "$target" reset --hard --quiet FETCH_HEAD
    fi
  else
    log "cloning $(basename "$target") @ $REF"
    git clone --quiet --depth 1 --branch "$REF" "$url" "$target"
    if printf '%s' "$REF" | grep -Eq '^v[0-9]'; then
      verify_release_tag "$target" "$REF"
    else
      log "warning: $REF is not a release tag; installing an unverified development ref"
    fi
  fi
}

clone_or_update "$PROTOCOL_URL" "$INSTALL_DIR/lucrium-protocol"
clone_or_update "$WORKER_URL" "$INSTALL_DIR/lucrium-worker"

log "syncing python environment"
(cd "$INSTALL_DIR/lucrium-worker" && uv sync --quiet --frozen --python "$PYTHON_VERSION")
chmod -R a+rX "$PYTHON_DIR" "$INSTALL_DIR/lucrium-worker/.venv"

WORKER_BIN="$INSTALL_DIR/lucrium-worker/.venv/bin/lucrium-worker"
VENV_PYTHON="$INSTALL_DIR/lucrium-worker/.venv/bin/python"
[ -x "$WORKER_BIN" ] || die "worker entrypoint not found: $WORKER_BIN"

run_as_service() {
  if command -v runuser >/dev/null 2>&1; then
    runuser -u "$SERVICE_USER" -- "$@"
  else
    su -s /bin/sh "$SERVICE_USER" -c "$(printf '%q ' "$@")"
  fi
}

# aiosellers ходит через tls_requests, который не содержит native library в
# wheel и скачивает её в site-packages при первом запросе. Под ProtectSystem=strict
# и root-owned venv запись невозможна, поэтому библиотеку готовим заранее.
log "preparing tls_requests native library"
mkdir -p "$TLS_DIR"
if [ -n "${LUCRIUM_TLS_LIBRARY:-}" ]; then
  [ -f "$LUCRIUM_TLS_LIBRARY" ] || die "LUCRIUM_TLS_LIBRARY not found: $LUCRIUM_TLS_LIBRARY"
  TLS_LIBRARY_PATH="$TLS_DIR/$(basename "$LUCRIUM_TLS_LIBRARY")"
  cp -f "$LUCRIUM_TLS_LIBRARY" "$TLS_LIBRARY_PATH"
else
  TLS_LIBRARY_PATH="$(
    "$VENV_PYTHON" - "$TLS_DIR" <<'PY'
import contextlib
import io
import shutil
import sys
from pathlib import Path

from tls_requests.models.libraries import TLSLibrary

destination_dir = Path(sys.argv[1])
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    path = TLSLibrary.download()
if not path:
    raise SystemExit("TLSLibrary.download() returned no path")
destination = destination_dir / Path(path).name
shutil.copy2(path, destination)
print(destination)
PY
  )" || die "cannot download tls_requests native library; set LUCRIUM_TLS_LIBRARY=/path/to/libtls-client.so"
fi
chmod 755 "$TLS_DIR"
chmod 644 "$TLS_LIBRARY_PATH"
run_as_service env TLS_LIBRARY_PATH="$TLS_LIBRARY_PATH" "$VENV_PYTHON" -c \
  "from tls_requests.models.libraries import TLSLibrary; TLSLibrary.load(); print('tls library ok')" \
  || die "tls_requests library is not loadable by $SERVICE_USER (check TLS_LIBRARY_PATH/permissions)"

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
Environment="TLS_LIBRARY_PATH=$TLS_LIBRARY_PATH"
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
if ! systemctl restart lucrium-worker.service; then
  systemctl status lucrium-worker.service --no-pager -l || true
  journalctl -u lucrium-worker.service -n 50 --no-pager || true
  die "worker service failed to start"
fi
for _ in $(seq 1 10); do
  systemctl is-active --quiet lucrium-worker.service && break
  sleep 1
done
if ! systemctl is-active --quiet lucrium-worker.service; then
  systemctl status lucrium-worker.service --no-pager -l || true
  journalctl -u lucrium-worker.service -n 50 --no-pager || true
  die "worker service is not active; check the logs above"
fi

TOKEN="$("$WORKER_BIN" show-token --config "$CONFIG_FILE")"
SCHEME="ws"; [ "$TLS_ENABLED" = "1" ] && [ -f "$CONFIG_DIR/tls/cert.pem" ] && SCHEME="wss"
URL_HOST="$IP"; case "$IP" in *:*) URL_HOST="[$IP]" ;; esac

echo
echo "============================================================"
echo " Lucrium Worker установлен и запущен"
echo "   Адрес: $SCHEME://$URL_HOST:$PORT"
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
