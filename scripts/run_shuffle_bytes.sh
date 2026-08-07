#!/usr/bin/env bash
# Shuffle volume per strategy, and what it would cost on a network.
#
#   ./scripts/run_shuffle_bytes.sh ~/ssb_synth 2>&1 | tee ~/shuffle.log
#   python3 scripts/summarize_shuffle.py results/shuffle_bytes.csv
#
# The manuscript states at joinless_v10_Jul_31.tex:1264 that in a multi-node
# deployment "shuffle traffic would traverse the network rather than local
# storage" and that the wall-clock effect "remains an empirical question,
# because broadcast, coordination, skew, scheduling and scaling overheads would
# also change". The volume is not a question. This measures it, so that the part
# of the answer arithmetic can supply is on the record.
#
# Numbers come from Spark's task metrics through a SparkListener in RunReport,
# not from the UI and not from parsing an event log after the fact.
#
# On a single node every shuffle read is local, so `shuffle_read_remote` is zero
# in every run here. That is the finding rather than a defect of it: the whole of
# the local volume is what would become remote in a distributed deployment.

set -uo pipefail

DATA="${1:-$HOME/ssb_synth}"
RUNS="${RUNS:-5}"
QUERIES="${QUERIES:-q1 q2 q3 q4}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
JPP="$HERE/jpp"
OUT="${OUT:-$ROOT/results/shuffle_bytes.csv}"
LOGDIR="${LOGDIR:-$ROOT/results/shuffle-logs}"
PLANDIR="${PLANDIR:-$ROOT/results/plans/shuffle}"

mkdir -p "$(dirname "$OUT")" "$LOGDIR" "$PLANDIR"
cd "$ROOT" || exit 1
export JPP_PLAN_DIR="$PLANDIR"

# Guard 2 of §16, before the sweep rather than after a surprise.
"$JPP" DatasetFingerprint "$DATA" 2>&1 | grep -E "^(FINGERPRINT|Exception)" \
  || { echo "fingerprint failed on $DATA; aborting" >&2; exit 1; }

[[ -s "$OUT" ]] || echo "query,strategy,run,status,elapsed_s,groups,total,shuffle_write_bytes,shuffle_write_records,shuffle_read_bytes,shuffle_read_local,shuffle_read_remote,shuffle_read_records,bhj,smj,shuffle_ops" > "$OUT"

field () {
  grep -m1 -E "^$1[[:space:]]*=" "$2" 2>/dev/null \
    | sed -e 's/^[^=]*=[[:space:]]*//' -e 's/[[:space:]]*s$//' -e 's/,/./g' -e 's/[[:space:]]*$//'
}

echo "=============================================================="
echo " shuffle volume   data=$DATA   runs=$RUNS   $(date)"
echo "=============================================================="

# Interleaved: repetition outermost. The host is shared, and a block layout would
# let a burst of unrelated load land on one strategy.
for r in $(seq 1 "$RUNS"); do
  for q in $QUERIES; do
    for strat in joinless bhj smj; do
      tag="${q}-${strat}-r${r}"
      log="$LOGDIR/${tag}.log"
      echo "----- $tag -----"
      if [[ "$strat" == "joinless" ]]; then
        JPP_RUN_LABEL="$tag" "$JPP" JoinlessQueries "$DATA" "$q" > "$log" 2>&1
      else
        JPP_RUN_LABEL="$tag" "$JPP" SparkBaselineQueries "$DATA" "$q" "$strat" > "$log" 2>&1
      fi
      rc=$?
      status="ok"; [[ $rc -ne 0 ]] && { status="fail:rc=$rc"; echo "  -> $status"; }

      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$q" "$strat" "$r" "$status" \
        "$(field elapsed "$log")" "$(field groups "$log")" "$(field 'total \(check\)' "$log")" \
        "$(field shuffle_write_bytes "$log")" "$(field shuffle_write_records "$log")" \
        "$(field shuffle_read_bytes "$log")" "$(field shuffle_read_local "$log")" \
        "$(field shuffle_read_remote "$log")" "$(field shuffle_read_records "$log")" \
        "$(field bhj_count "$log")" "$(field smj_count "$log")" "$(field shuffle_count "$log")" >> "$OUT"
    done
  done
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " csv:  $OUT"
echo " next: python3 scripts/summarize_shuffle.py $OUT"
echo "=============================================================="
