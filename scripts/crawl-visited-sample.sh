#!/usr/bin/env bash
set -euo pipefail
BASE="${CRAWLER_API:-http://localhost:3000}"
RUN_ID="${1:?usage: crawl-visited-sample.sh <run_id>}"
LIMIT="${2:-15}"
curl -sS "${BASE}/crawl-runs/${RUN_ID}/urls?status=VISITED&limit=${LIMIT}&sort=visited_at&order=desc" | jq .
