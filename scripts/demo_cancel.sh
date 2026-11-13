#!/usr/bin/env bash
# Demo: cancel a slow upload — confirmation only after writes stopped,
# files deleted and quota released; nothing may be written afterwards.
set -u
BASE="${1:-http://127.0.0.1:8080}"
SAMPLES="$(cd "$(dirname "$0")/.." && pwd)/samples"

BID=$(curl -s -X POST "$BASE/api/batches" -H 'Content-Type: application/json' \
      -d '{"sample_name":"SLOWCANCEL001"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')
echo "batch: $BID"

# Throttled upload in the background (~30 KB/s of the 640 KB oversize gz).
( curl -s -X PUT --limit-rate 30k --data-binary @"$SAMPLES/oversize_r1.fq.gz" \
    -H 'Content-Type: application/gzip' "$BASE/api/batches/$BID/r1"
  echo " <- upload request finished" ) &
UP_PID=$!
sleep 3
echo "mid-upload state: $(curl -s "$BASE/api/batches/$BID")"

echo "cancelling..."
time curl -s -X POST "$BASE/api/batches/$BID/cancel"
echo
wait $UP_PID
echo "final state: $(curl -s "$BASE/api/batches/$BID" | python3 -m json.tool)"
echo "quota: $(curl -s "$BASE/api/quota")"
echo "upload after cancel (expect 409):"
curl -s -o /dev/null -w '%{http_code}\n' -X PUT \
  --data-binary @"$SAMPLES/normal_r2.fq.gz" "$BASE/api/batches/$BID/r2"
