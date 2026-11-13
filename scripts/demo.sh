#!/usr/bin/env bash
# Demo: normal / mismatched / corrupt / multi-member / oversize batches.
# Usage: ./scripts/demo.sh [base_url]     (default http://127.0.0.1:8080)
set -u
BASE="${1:-http://127.0.0.1:8080}"
SAMPLES="$(cd "$(dirname "$0")/.." && pwd)/samples"

json_field() { python3 -c "import sys,json;print(json.load(sys.stdin).get('$1',''))"; }

wait_final() {  # $1=batch_id -> prints final status json
  local S ST
  for _ in $(seq 1 120); do
    S=$(curl -s "$BASE/api/batches/$1")
    ST=$(echo "$S" | json_field status)
    case "$ST" in PASSED|FAILED|CANCELLED) echo "$S"; return;; esac
    sleep 0.5
  done
  echo "$S"
}

LAST_BID=""
run_case() {  # $1=sample_name $2=r1_file $3=r2_file
  local BID FINAL ERR
  BID=$(curl -s -X POST "$BASE/api/batches" -H 'Content-Type: application/json' \
        -d "{\"sample_name\":\"$1\"}" | json_field batch_id)
  curl -s -X PUT --data-binary "@$2" -H 'Content-Type: application/gzip' \
       "$BASE/api/batches/$BID/r1" > /dev/null
  curl -s -X PUT --data-binary "@$3" -H 'Content-Type: application/gzip' \
       "$BASE/api/batches/$BID/r2" > /dev/null
  FINAL=$(wait_final "$BID")
  echo "== $1 -> $(echo "$FINAL" | json_field status)"
  ERR=$(echo "$FINAL" | json_field error)
  [ "$ERR" != "None" ] && [ -n "$ERR" ] && echo "   error: $ERR"
  LAST_BID="$BID"
}

echo "quota: $(curl -s "$BASE/api/quota")"
echo
run_case NORMAL001   "$SAMPLES/normal_r1.fq.gz"      "$SAMPLES/normal_r2.fq.gz"
NORMAL_BID="$LAST_BID"
run_case MISMATCH001 "$SAMPLES/mismatch_r1.fq.gz"    "$SAMPLES/mismatch_r2.fq.gz"
run_case CORRUPT001  "$SAMPLES/corrupt_r1.fq.gz"     "$SAMPLES/normal_r2.fq.gz"
run_case MULTI001    "$SAMPLES/multimember_r1.fq.gz" "$SAMPLES/normal_r2.fq.gz"
run_case OVERSIZE001 "$SAMPLES/oversize_r1.fq.gz"    "$SAMPLES/oversize_r2.fq.gz"

echo
echo "== download expanded FASTQ of the PASSED batch =="
curl -s "$BASE/api/batches/$NORMAL_BID/r1.fastq" | head -4
echo "..."
echo
echo "quota after all cases: $(curl -s "$BASE/api/quota")"
echo "(only the PASSED batch keeps files; failed batches were reclaimed)"
