#!/usr/bin/env bash
# End-to-end smoke test of the docker compose stack.
#   cp .env.example .env && scripts/smoke_test.sh [event-count]
# Starts the stack, publishes synthetic events (with duplicates and malformed
# messages), then checks PostgreSQL, the API and Prometheus against the
# producer's own report. Tears everything down on exit.
set -euo pipefail

COUNT="${1:-3000}"
PROJECT="rsa-smoke"
set -a; . ./.env; set +a
API="http://127.0.0.1:${API_HOST_PORT:-8000}"
PROM="http://127.0.0.1:${PROMETHEUS_HOST_PORT:-9090}"

dc() { docker compose -p "$PROJECT" "$@"; }
sql() { dc exec -T postgres psql -U "${POSTGRES_USER:-rsa}" -d "${POSTGRES_DB:-rsa}" -Atc "$1"; }
fail() { echo "FAIL: $*" >&2; dc logs --tail 50 consumer >&2 || true; exit 1; }
trap 'dc --profile demo down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

dc up -d --wait
report=$(dc --profile demo run --rm --no-deps producer produce \
  --count="$COUNT" --rate=500 --seed=7 --duplicate-rate=0.02 --malformed-rate=0.01 | tail -n 1)
echo "producer: $report"
malformed=$(python3 -c 'import json,sys; r=json.loads(sys.argv[1]); assert r["failed"] == 0; print(r["malformed_injected"])' "$report")

deadline=$((SECONDS + 120))
until [ "$(sql "SELECT count(*) FROM processed_events")" = "$COUNT" ] \
   && [ "$(sql "SELECT count(*) FROM dead_letters")" = "$malformed" ]; do
  [ "$SECONDS" -lt "$deadline" ] || fail "database did not reach $COUNT events / $malformed dead letters"
  sleep 2
done
echo "database: $COUNT unique events, $malformed dead letters (duplicates ignored)"

health=$(curl -fsS --noproxy '*' "$API/api/v1/pipeline/health?window_minutes=60")
python3 - "$health" "$COUNT" "$malformed" <<'PY'
import json, sys
h, count, malformed = json.loads(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
assert h["events_ingested"] == count, h
assert h["dead_letters"] == malformed, h
print(f"api: events={h['events_ingested']} dead_letters={h['dead_letters']} "
      f"p50={h['latency_ms_p50']}ms p95={h['latency_ms_p95']}ms (observed, not a benchmark)")
PY
curl -fsS --noproxy '*' "$API/api/v1/production/summary" | python3 -c 'import json,sys; assert json.load(sys.stdin), "empty production summary"'

deadline=$((SECONDS + 60))
until curl -fsS --noproxy '*' "$PROM/api/v1/targets" \
  | python3 -c 'import json,sys; t=json.load(sys.stdin)["data"]["activeTargets"]; sys.exit(0 if t and all(x["health"]=="up" for x in t) else 1)'; do
  [ "$SECONDS" -lt "$deadline" ] || fail "Prometheus targets not up"
  sleep 3
done
echo "prometheus: all scrape targets up"
echo "SMOKE TEST PASSED"
