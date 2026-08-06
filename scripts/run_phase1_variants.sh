#!/usr/bin/env bash
# The two Phase 1 implementations, across dimension scales.
#
#   ./scripts/run_phase1_variants.sh ~/ssb_medium 2>&1 | tee ~/phase1.log
#
#   collect     the published implementation: the dimension is collected to the
#               driver and encoded there. Fails at a 1 GB dimension, first on
#               spark.driver.maxResultSize and then, with that raised, on driver
#               heap.
#   streaming   the same structure filled from toLocalIterator, one partition at
#               a time. Peak driver memory is the array plus one partition
#               instead of the array plus the whole dimension.
#
# Only Phase 1 differs. The kernels, the fact-table scan and the aggregation are
# the same code in both, so a difference here is a difference in Phase 1 and
# nothing else.
#
# `setup` and `scan` are reported separately by JoinlessQueries, which is what
# makes the comparison informative: the interesting quantity is not only whether
# the run completes but how much of its time Phase 1 takes once it does.
#
# Failures are results and are recorded as rows.

set -uo pipefail

MEDIUM="${1:-$HOME/ssb_medium}"
RUNS="${RUNS:-5}"
QUERY="${QUERY:-q1}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
JPP="$HERE/jpp"
OUT="${OUT:-$ROOT/results/phase1_variants.csv}"
LOGDIR="${LOGDIR:-$ROOT/results/phase1-logs}"
PLANDIR="${PLANDIR:-$ROOT/results/plans/phase1}"
SIZES="${SIZES:-dim10mb dim100mb dim1000mb}"

mkdir -p "$(dirname "$OUT")" "$LOGDIR" "$PLANDIR"
cd "$ROOT" || exit 1
export JPP_PLAN_DIR="$PLANDIR"

[[ -s "$OUT" ]] || echo "dimension_set,variant,query,run,status,elapsed_s,setup_s,scan_s,groups,total,failure" > "$OUT"

field () {
  grep -m1 -E "^$1[[:space:]]*=" "$2" 2>/dev/null \
    | sed -e 's/^[^=]*=[[:space:]]*//' -e 's/[[:space:]]*s$//' -e 's/,/./g' -e 's/[[:space:]]*$//'
}

echo "=============================================================="
echo " phase 1 variants   sizes=$SIZES   runs=$RUNS   $(date)"
echo "=============================================================="

# Interleaved: repetition outermost, so unrelated load on this shared host is
# spread across variants rather than landing on one of them.
for r in $(seq 1 "$RUNS"); do
  for size in $SIZES; do
    dir="$MEDIUM/$size"
    [[ -d "$dir" ]] || { echo "SKIP $size (no such directory)"; continue; }
    for variant in collect streaming; do
      tag="${size}-${variant}-${QUERY}-r${r}"
      log="$LOGDIR/${tag}.log"
      echo "----- $tag -----"
      JPP_PHASE1="$variant" JPP_RUN_LABEL="$tag" \
        "$JPP" JoinlessQueries "$dir" "$QUERY" > "$log" 2>&1
      rc=$?

      status="ok"; failure=""
      if [[ $rc -ne 0 ]]; then
        if   grep -qi "maxResultSize" "$log"; then status="fail"; failure="maxResultSize"
        elif grep -qi "OutOfMemoryError" "$log"; then status="fail"; failure="OOM"
        else status="fail"; failure="rc=$rc"
        fi
        echo "  -> $status:$failure"
      fi

      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$size" "$variant" "$QUERY" "$r" "$status" \
        "$(field elapsed "$log")" "$(field setup "$log")" "$(field scan "$log")" \
        "$(field groups "$log")" "$(field 'total \(check\)' "$log")" "$failure" >> "$OUT"
    done
  done
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " csv:  $OUT"
echo "=============================================================="
