# shared helpers for scripts/*.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$ROOT/infrastructure"
cd "$INFRA"
if [[ ! -f .env ]]; then
  echo "infrastructure/.env not found -> creating it from .env.example (LOCAL demo credentials)"
  cp .env.example .env
fi
set -a; source .env; set +a

CORE=(-f docker-compose.yml)
TRINO=(-f docker-compose.trino.yml)
KYUUBI=(-f docker-compose.kyuubi.yml)
ALL=("${CORE[@]}" "${TRINO[@]}" "${KYUUBI[@]}")

# always load all three compose files so overlay containers are never reported as orphans; "up" names services explicitly
dc() { docker compose "${ALL[@]}" "$@"; }
log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }

# resolve --engine trino|kyuubi|both -> compose file list in COMPOSE_FILES, engine list in ENGINES
parse_engine() {
  local engine="${1:-both}"
  case "$engine" in
    trino)  COMPOSE_FILES=("${CORE[@]}" "${TRINO[@]}");  ENGINES=(trino) ;;
    kyuubi) COMPOSE_FILES=("${CORE[@]}" "${KYUUBI[@]}"); ENGINES=(kyuubi) ;;
    both)   COMPOSE_FILES=("${ALL[@]}");                 ENGINES=(trino kyuubi) ;;
    core)   COMPOSE_FILES=("${CORE[@]}");                ENGINES=() ;;
    *) echo "unknown engine '$engine' (trino|kyuubi|both|core)"; exit 2 ;;
  esac
}

# generate the local-only credential files that are git-ignored (Trino password file, group file, prometheus secret)
generate_local_credentials() {
  mkdir -p trino/generated monitoring/generated
  local pw="trino/generated/password.db"
  : > "$pw"
  local u p
  for u in admin etl analyst1 analyst2 restricted1 bench monitor; do
    case "$u" in
      admin) p="$ADMIN_PASSWORD" ;; etl) p="$ETL_PASSWORD" ;; analyst1|analyst2) p="$ANALYST_PASSWORD" ;;
      restricted1) p="$RESTRICTED_PASSWORD" ;; bench) p="$BENCH_PASSWORD" ;; monitor) p="$ADMIN_PASSWORD" ;;
    esac
    # bcrypt ($2y$, cost 10) as required by Trino's file password authenticator
    dc run --rm --no-deps -T tools htpasswd -nbBC 10 "$u" "$p" >> "$pw"
  done
  sed -i 's/\r$//' "$pw"
  cat > trino/generated/groups.txt <<GROUPS
admins:admin
etl:etl
analysts:analyst1,analyst2,bench
restricted_analysts:restricted1
monitors:monitor
GROUPS
  printf '%s' "$ADMIN_PASSWORD" > monitoring/generated/trino_monitor_password
  # self-signed TLS certificate for Trino HTTPS (PEM with key + cert). PROD: CA-issued certificate.
  if [[ ! -f trino/generated/trino.pem ]]; then
    dc run --rm --no-deps -T tools sh -c "openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj '/CN=trino' \
      -addext 'subjectAltName=DNS:trino,DNS:localhost,IP:127.0.0.1' -keyout /tmp/k.pem -out /tmp/c.pem 2>/dev/null && cat /tmp/k.pem /tmp/c.pem" \
      | sed 's/\r$//' > trino/generated/trino.pem
  fi
  echo "generated trino/generated/{password.db,groups.txt,trino.pem} and monitoring/generated/trino_monitor_password"
}

wait_healthy() {  # wait_healthy <service> <timeout-seconds>
  local svc="$1" timeout="${2:-300}" t=0 cid status
  while (( t < timeout )); do
    cid=$(dc ps -q "$svc" 2>/dev/null || true)
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo "missing")
    if [[ "$status" == "healthy" || "$status" == "running" && "$svc" == "ranger-admin-stub" ]]; then echo "$svc: $status"; return 0; fi
    if [[ "$status" == "exited" || "$status" == "dead" ]]; then echo "$svc: $status"; dc logs --tail 50 "$svc"; return 1; fi
    sleep 5; t=$((t+5))
  done
  echo "timeout waiting for $svc (last status: $status)"; dc logs --tail 80 "$svc"; return 1
}
