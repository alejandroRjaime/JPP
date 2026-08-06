#!/usr/bin/env bash
# Does the pattern's broadcast cost amortise across a session?
#
#   ./scripts/run_session_reuse.sh ~/ssb_synth 2>&1 | tee ~/session.log
#
# The paper argues that the pattern broadcasts once per session while a
# relational plan rebuilds and rebroadcasts per query. Every other benchmark in
# this repository runs one query per JVM, so that saving cannot appear in any of
# their numbers: the claim has never been measured here. This measures it.
#
# Each invocation of SessionReuse runs Q1, Q2, Q3 and Q4 inside a single
# SparkSession and reports, per query, the elapsed time, the Phase 1 time it
# paid (zero if a previous query already built the structure), and the broadcast
# milliseconds Spark charged it.
#
# Three regimes. `relational-cached` is the obvious objection to `relational`:
# cache the dimensions up front and the per-query cost of reading them goes away.
# Whether the per-query *broadcast* goes away with it is the question, and it is
# measured rather than assumed.
#
# The two regimes alternate rather than running in blocks, for the usual reason:
# the host carries unrelated load, and a block layout would let a burst land
# entirely on one regime.
#
# The decisive quantity is not the session total but the cumulative broadcast
# cost: the total also contains scan time, which is the same work in both
# regimes and would mask the effect being tested.

set -uo pipefail

DATA="${1:-$HOME/ssb_synth}"
RUNS="${RUNS:-5}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
JPP="$HERE/jpp"
OUT="${OUT:-$ROOT/results/session_reuse.csv}"
LOGDIR="${LOGDIR:-$ROOT/results/session-logs}"
PLANDIR="${PLANDIR:-$ROOT/results/plans/session}"

mkdir -p "$(dirname "$OUT")" "$LOGDIR" "$PLANDIR"
cd "$ROOT" || exit 1
export JPP_PLAN_DIR="$PLANDIR"

echo "=============================================================="
echo " session reuse   data=$DATA   runs=$RUNS   $(date)"
echo "=============================================================="

for r in $(seq 1 "$RUNS"); do
  for regime in joinless relational relational-cached relational-cached-hi; do
    tag="session-${regime}-r${r}"
    echo "----- $tag -----"
    JPP_RUN_LABEL="$tag" "$JPP" SessionReuse "$DATA" "$regime" "$OUT" \
      > "$LOGDIR/${tag}.log" 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]; then
      echo "  -> FAILED rc=$rc (see $LOGDIR/${tag}.log)"
    else
      grep -E "^(session total|phase1 total|bcast total)" "$LOGDIR/${tag}.log" | sed 's/^/  /'
    fi
  done
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " csv:  $OUT"
echo "=============================================================="
echo
echo "Note: the CSV carries every repetition. The first repetition of each"
echo "regime is warm-up and is discarded when the medians are taken, as"
echo "everywhere else in this repository."
