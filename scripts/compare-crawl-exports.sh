#!/usr/bin/env bash
set -euo pipefail
# Compare normalized URL sets from two JSON exports (files produced via export API).
# Usage: compare-crawl-exports.sh run1.json run2.json
A="${1:?first export json}"
B="${2:?second export json}"
jq -r '.urls[].normalized_url' "$A" | sort -u > /tmp/crawl_a_urls.txt
jq -r '.urls[].normalized_url' "$B" | sort -u > /tmp/crawl_b_urls.txt
echo "only in first:"
comm -23 /tmp/crawl_a_urls.txt /tmp/crawl_b_urls.txt | head
echo "only in second:"
comm -13 /tmp/crawl_a_urls.txt /tmp/crawl_b_urls.txt | head
echo "counts:" "$(wc -l < /tmp/crawl_a_urls.txt)" "$(wc -l < /tmp/crawl_b_urls.txt)"
