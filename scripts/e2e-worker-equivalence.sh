#!/usr/bin/env bash
# Compare crawl exports with worker=1 vs worker=N (same fixture, same machine).
# Prereq: docker compose stack from repo root; CRAWLER_API defaults to http://localhost:3000
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
API="${CRAWLER_API:-http://localhost:3000}"
OUT_DIR="${TMPDIR:-/tmp}/e2e-worker-equiv-$$"
mkdir -p "$OUT_DIR"

echo "==> Scaling worker=1"
docker compose up -d --build --scale worker=1

echo "==> Waiting for control-plane health"
for i in $(seq 1 90); do
  if curl -sf "${API}/health" >/dev/null; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 90 ]]; then
    echo "timeout waiting for ${API}/health" >&2
    exit 1
  fi
done

echo "==> Export (worker=1)"
CRAWLER_API="$API" npx tsx tests/helpers/e2e-run-export-cli.ts --fixture dupes --out "$OUT_DIR/w1.json"

echo "==> Scaling worker=3"
docker compose up -d --scale worker=3
sleep 5

echo "==> Export (worker=3)"
CRAWLER_API="$API" npx tsx tests/helpers/e2e-run-export-cli.ts --fixture dupes --out "$OUT_DIR/w3.json"

echo "==> Comparing normalized URL sets"
npm run compare-results -- "$OUT_DIR/w1.json" "$OUT_DIR/w3.json"
echo "OK: exports match (see compare-results output above)"
