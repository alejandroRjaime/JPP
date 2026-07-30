#!/usr/bin/env bash
# Disaggregated-storage suite: the same queries against the object store.
#
#   export S3_SECRET=...
#   ./scripts/run_all_remote.sh s3a://ssb-benchmark/sf600 2>&1 | tee ~/remote.log
#   python3 scripts/summarize.py ~/remote.log
#
# Several hours. Each run transfers the fact table over the storage link, so
# wall-clock time is dominated by bandwidth rather than by the strategy. RUNS
# defaults to 3 here rather than 5 for that reason; raise it if time allows.
#
# Q5 is not included: the year-partitioned fact table exists only locally, and
# uploading another 24 GB over the link is not worth the pruning measurement.

set -uo pipefail

DATA="${1:-s3a://ssb-benchmark/sf600}"
RUNS="${RUNS:-3}"
HERE="$(cd "$(dirname "$0")" && pwd)"
JPP="$HERE/jpp"

if [[ -z "${S3_SECRET:-}" ]]; then
  echo "set S3_SECRET first" >&2
  exit 1
fi

echo "=============================================================="
echo " remote suite   data=$DATA   runs=$RUNS   $(date)"
echo "=============================================================="

for q in q1 q2 q3 q4; do
  for r in $(seq 1 "$RUNS"); do
    echo "----- $q  run $r/$RUNS -----"
    "$JPP" JoinlessQueries      "$DATA" "$q"
    "$JPP" SparkBaselineQueries "$DATA" "$q" smj
    "$JPP" SparkBaselineQueries "$DATA" "$q" bhj
  done
done

echo
echo "=============================================================="
echo " done  $(date)"
echo "=============================================================="
