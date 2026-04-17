#!/usr/bin/env bash
set -euo pipefail
BASE="${CRAWLER_API:-http://localhost:3000}"
SEED="${1:-}"
if [[ -z "$SEED" ]]; then
  echo "Usage: $(basename "$0") <seedUrl>" >&2
  echo "Example: $(basename "$0") 'https://example.com/'" >&2
  echo "Assignment-style demo: $(basename "$0") 'https://ipfabric.io/'" >&2
  exit 1
fi
BODY="$(node -e "console.log(JSON.stringify({ seedUrl: process.argv[1] }))" "$SEED")"
curl -sS -X POST "${BASE}/crawl-runs" -H "Content-Type: application/json" -d "$BODY"
echo
