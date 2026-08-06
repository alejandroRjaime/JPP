#!/usr/bin/env bash
# Broadcast-threshold sweep.
#
#   ./scripts/run_threshold_sweep.sh ~/ssb_synth 2>&1 | tee ~/sweep.log
#
# Answers the objection that raising `spark.sql.autoBroadcastJoinThreshold`
# would let Spark broadcast the dimensions into memory and make the pattern
# unnecessary. It is measured rather than argued, and three things are recorded
# at every point, because the time alone does not settle it:
#
#   1. the time, under the usual median protocol;
#   2. the plan Spark actually chose. Raising a threshold is not the same as
#      changing a plan — Spark may leave it untouched — so without the operator
#      counts a flat curve cannot be distinguished from a threshold that did
#      nothing;
#   3. the broadcast cost separately from the total. This is the substantive
#      point: the pattern broadcasts once per session, a relational plan rebuilds
#      and rebroadcasts per query, so over a four-query set that is four
#      broadcasts against one.
#
# Failures are results. An OOM or an exceeded maxResultSize at a high threshold
# is exactly what the sweep is for, and is recorded as a row rather than
# discarded.
#
# The `auto` strategy leaves the threshold as passed and applies no hint, so the
# optimizer decides. The `joinless` rows are the pattern, whose threshold is
# fixed at -1 by construction and appears once per query as the reference.
#
# Runs are interleaved: the outer loop is the repetition, the inner loops the
# configurations. The host is not dedicated, so a block layout would let a burst
# of unrelated load land on one configuration and be indistinguishable from a
# property of that configuration.

set -uo pipefail

DATA="${1:-$HOME/ssb_synth}"
RUNS="${RUNS:-5}"
QUERIES="${QUERIES:-q1 q2 q3 q4}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
JPP="$HERE/jpp"
OUT="${OUT:-$ROOT/results/threshold_sweep.csv}"
DIMCSV="${DIMCSV:-$ROOT/results/dimension_sizes.csv}"
PLANDIR="${PLANDIR:-$ROOT/results/plans}"
LOGDIR="${LOGDIR:-$ROOT/results/sweep-logs}"

mkdir -p "$(dirname "$OUT")" "$PLANDIR" "$LOGDIR"
cd "$ROOT" || exit 1

# Plan files go where this sweep was told to put them. Without this every sweep
# writes to the same default directory, and a sweep over a different dataset
# silently overwrites the plan files of an earlier one — the run labels collide
# because they are built from the query and threshold, which are the same.
export JPP_PLAN_DIR="$PLANDIR"

# --- 1. dimension sizes, and the thresholds derived from them ----------------
# The thresholds are derived from the optimizer's estimate of each projected
# dimension, which is the quantity the threshold is compared against. Round
# numbers would not tell us which dimensions each point admits.
if [[ ! -s "$DIMCSV" || -n "${FORCE_DIMSTATS:-}" ]]; then
  echo "===== measuring dimension sizes ====="
  "$JPP" DimStats "$DATA" "$DIMCSV" || { echo "DimStats failed" >&2; exit 1; }
fi

mapfile -t SWEEP < <(python3 - "$DIMCSV" <<'PY'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
est = sorted(((r["dimension"], int(r["est_projected_bytes"])) for r in rows), key=lambda x: x[1])
pts = [(10 * 1024 * 1024, "default-10MiB")]
run = []
for name, b in est:
    run.append(name)
    # One byte above this dimension's estimate: the smallest value that admits
    # it and every smaller dimension.
    pts.append((b + 1, f"admits-{len(run)}:{'+'.join(run)}"))
if est:
    # Clearly above every dimension, to separate "the threshold was too low"
    # from "the plan does not change however high it goes".
    pts.append((est[-1][1] * 8, "above-all-8x"))
seen = set()
for b, label in pts:
    if b in seen:
        continue
    seen.add(b)
    print(f"{b} {label}")
PY
)

# SWEEP_ONLY restricts which threshold points are run, as a space-separated list
# of label prefixes. Used when the question is where a curve crosses rather than
# what the whole curve looks like: the points dropped must be ones that were never
# the best relational configuration at any measured scale, or the "best relational"
# column would be reporting the best of an arbitrary subset. What is dropped is
# logged, so a reader is not left to infer coverage from silence.
if [[ -n "${SWEEP_ONLY:-}" ]]; then
  kept=(); dropped=()
  for pt in "${SWEEP[@]}"; do
    lab="${pt#* }"; keep=""
    for want in $SWEEP_ONLY; do [[ "$lab" == "$want"* ]] && keep=1; done
    if [[ -n "$keep" ]]; then kept+=("$pt"); else dropped+=("$lab"); fi
  done
  SWEEP=("${kept[@]}")
  echo "SWEEP_ONLY active: dropped ${#dropped[@]} point(s): ${dropped[*]}"
fi

if [[ ${#SWEEP[@]} -eq 0 ]]; then
  echo "could not derive sweep points from $DIMCSV" >&2
  exit 1
fi

echo "===== sweep points ====="
printf '  %s\n' "${SWEEP[@]}"

# --- 2. CSV header -----------------------------------------------------------
if [[ ! -s "$OUT" ]]; then
  echo "query,strategy,threshold_bytes,threshold_label,run,status,elapsed_s,setup_s,groups,total,bhj,smj,shuffle,bcast_ex,exchanges,bcast_ms,bcast_bytes,plan_file" > "$OUT"
fi

# Pull "key = value" out of a run log. Locale may render decimals with a comma,
# so the value is normalised here rather than assumed to be dot-separated.
field () {
  local key="$1" file="$2"
  grep -m1 -E "^${key}[[:space:]]*=" "$file" 2>/dev/null \
    | sed -e 's/^[^=]*=[[:space:]]*//' -e 's/[[:space:]]*s$//' -e 's/,/./g' -e 's/[[:space:]]*$//'
}

run_point () {
  local q="$1" strat="$2" thr="$3" label="$4" run="$5"
  local tag="${q}-${strat}-${label}-r${run}"
  local log="$LOGDIR/${tag}.log"

  echo "----- $tag -----"
  if [[ "$strat" == "joinless" ]]; then
    JPP_RUN_LABEL="$tag" JPP_EXTRA_CONF="" \
      "$JPP" JoinlessQueries "$DATA" "$q" > "$log" 2>&1
  else
    JPP_RUN_LABEL="$tag" JPP_EXTRA_CONF="spark.sql.autoBroadcastJoinThreshold=$thr" \
      "$JPP" SparkBaselineQueries "$DATA" "$q" "$strat" > "$log" 2>&1
  fi
  local rc=$?

  local status="ok"
  if [[ $rc -ne 0 ]]; then
    # Distinguish the failure modes the sweep is meant to surface from a generic
    # crash, so that the CSV says which one happened.
    if   grep -qi "maxResultSize" "$log"; then status="fail:maxResultSize"
    elif grep -qi "OutOfMemoryError\|java.lang.OutOfMemory" "$log"; then status="fail:OOM"
    elif grep -qi "GC overhead limit" "$log"; then status="fail:GC"
    else status="fail:rc=$rc"
    fi
    echo "  -> $status (see $log)"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$q" "$strat" "$thr" "$label" "$run" "$status" \
    "$(field elapsed "$log")" "$(field setup "$log")" \
    "$(field groups "$log")" "$(field 'total \(check\)' "$log")" \
    "$(field bhj_count "$log")" "$(field smj_count "$log")" \
    "$(field shuffle_count "$log")" "$(field bcast_ex "$log")" \
    "$(field 'exchange_count' "$log")" "$(field bcast_ms "$log")" \
    "$(field bcast_bytes "$log")" "$(field plan_file "$log")" >> "$OUT"
}

# --- 3. the sweep ------------------------------------------------------------
echo "=============================================================="
echo " threshold sweep   data=$DATA   runs=$RUNS   $(date)"
echo "=============================================================="

for r in $(seq 1 "$RUNS"); do
  for q in $QUERIES; do
    # The pattern: one reference point per query per repetition.
    run_point "$q" joinless -1 "pattern" "$r"
    for pt in "${SWEEP[@]}"; do
      thr="${pt%% *}"; label="${pt#* }"
      run_point "$q" auto "$thr" "$label" "$r"
    done
  done
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " csv:   $OUT"
echo " plans: $PLANDIR"
echo " next:  python3 scripts/summarize_sweep.py $OUT"
echo "=============================================================="
