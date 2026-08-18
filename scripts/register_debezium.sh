#!/usr/bin/env bash
# Registers the Debezium Postgres connector with Kafka Connect.
#
# The password lives in docker/.env, never in the connector JSON, so the
# committed config carries a REPLACED_AT_REGISTRATION placeholder and this
# script substitutes it at call time.
#
#   ./scripts/register_debezium.sh            register (or re-register)
#   ./scripts/register_debezium.sh status     show connector + task state
#   ./scripts/register_debezium.sh delete     remove the connector
#
# Deleting the connector does NOT drop the Postgres replication slot. An
# orphaned slot pins the WAL and will eventually fill the disk — see
# `./scripts/drop_slot.sh` and COST.md trap #9.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT}/docker/.env"
CONNECTOR_JSON="${ROOT}/docker/debezium/connectors/northpeak-erp.json"
CONNECTOR_NAME="northpeak-erp-connector"

[[ -f "${ENV_FILE}" ]] || { echo "missing ${ENV_FILE} — copy docker/.env.example first"; exit 1; }
# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

CONNECT_URL="http://localhost:${CONNECT_PORT:-8083}"

case "${1:-register}" in
  status)
    curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | python -m json.tool
    exit 0
    ;;
  delete)
    curl -sf -X DELETE "${CONNECT_URL}/connectors/${CONNECTOR_NAME}"
    echo "connector deleted. Replication slot 'northpeak_slot' still exists — drop it if you are done."
    exit 0
    ;;
esac

echo "waiting for Kafka Connect at ${CONNECT_URL} ..."
for i in $(seq 1 60); do
  if curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; then break; fi
  [[ $i -eq 60 ]] && { echo "Kafka Connect did not become ready"; exit 1; }
  sleep 2
done

payload="$(python - "$CONNECTOR_JSON" "$DEBEZIUM_USER" "$DEBEZIUM_PASSWORD" <<'PY'
import json, sys, pathlib
cfg = json.loads(pathlib.Path(sys.argv[1]).read_text())
cfg["config"]["database.user"] = sys.argv[2]
cfg["config"]["database.password"] = sys.argv[3]
print(json.dumps(cfg))
PY
)"

# PUT on /config is idempotent: it creates or updates. POST /connectors errors
# with 409 if the connector already exists, which makes re-runs annoying.
echo "${payload}" \
  | python -c "import json,sys; print(json.dumps(json.load(sys.stdin)['config']))" \
  | curl -sf -X PUT -H 'Content-Type: application/json' \
      --data @- "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  | python -m json.tool

echo
echo "registered. Checking task state in 5s ..."
sleep 5
curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | python -m json.tool
