#!/usr/bin/env bash
set -euo pipefail
BASE="${CRAWLER_API:-http://localhost:3000}"
RUN_ID="${1:?usage: crawl-summary.sh <run_id>}"
curl -sS "${BASE}/crawl-runs/${RUN_ID}/summary" | jq .
