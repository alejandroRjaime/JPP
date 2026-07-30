#!/usr/bin/env bash
# Complete local measurement suite. Everything the paper's tables depend on,
# in one run, logged to a single file.
#
#   ./scripts/run_all_local.sh ~/ssb_synth 2>&1 | tee ~/local.log
#   python3 scripts/summarize.py ~/local.log
#
# Roughly 45 minutes. Each configuration is executed RUNS times; the first is
# discarded as warm-up and the median of the rest is reported, which is the
# protocol described in the paper.

set -uo pipefail

DATA="${1:-$HOME/ssb_synth}"
RUNS="${RUNS:-5}"
HERE="$(cd "$(dirname "$0")" && pwd)"
JPP="$HERE/jpp"

echo "=============================================================="
echo " local suite   data=$DATA   runs=$RUNS   $(date)"
echo "=============================================================="

# --- 1. Query set: Q1-Q4, three Spark strategies -----------------------------
# Feeds: Table 1 (per-system times), the query-set results table, and the
# cost-model validation table.
for q in q1 q2 q3 q4; do
  for r in $(seq 1 "$RUNS"); do
    echo "----- $q  run $r/$RUNS -----"
    "$JPP" JoinlessQueries      "$DATA" "$q"
    "$JPP" SparkBaselineQueries "$DATA" "$q" smj
    "$JPP" SparkBaselineQueries "$DATA" "$q" bhj
  done
done

# --- 2. Q5: partition pruning ------------------------------------------------
# Feeds: the pruning table. Requires lineorder_by_year, produced by PartitionFact.
if [[ -d "$DATA/lineorder_by_year" ]]; then
  for r in $(seq 1 "$RUNS"); do
    echo "----- q5  run $r/$RUNS -----"
    "$JPP" JoinlessQueries      "$DATA" q5
    "$JPP" SparkBaselineQueries "$DATA" q5 smj
    "$JPP" SparkBaselineQueries "$DATA" q5 bhj
  done
else
  echo "SKIP q5: $DATA/lineorder_by_year not found."
  echo "       run:  ./scripts/jpp PartitionFact $DATA"
fi

# --- 3. Base variant ---------------------------------------------------------
# Feeds: the implementation optimization path table (the unoptimized row).
for r in $(seq 1 "$RUNS"); do
  echo "----- base variant  run $r/$RUNS -----"
  "$JPP" JoinlessBase "$DATA"
done

# --- 4. Fact-table redistribution --------------------------------------------
# Feeds: the repartition table. Q1 with an exchange before the scan.
for r in $(seq 1 "$RUNS"); do
  echo "----- q1 repartition=64  run $r/$RUNS -----"
  "$JPP" JoinlessQueries "$DATA" q1 16 64
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " next:  python3 scripts/summarize.py <this log>"
echo "=============================================================="
