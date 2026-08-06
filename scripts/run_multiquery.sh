#!/usr/bin/env bash
# Scenario expansion: `.par` against a single combined query.
#
#   ./scripts/run_multiquery.sh ~/ssb_scenarios 2>&1 | tee ~/multiquery.log
#
# Answers a reviewer comment: that the paper collapses multiple scenarios into
# one execution with Scala's `.par`, that this resembles multi-query
# optimization, and that combining the queries into one should have been tried
# and compared.
#
# Three variants, interleaved as always:
#
#   par   the published implementation
#   seq   the same loop with the parallel collection removed, to establish what
#         `.par` itself contributes — a question the paper never answered and
#         which neither of the other two variants can answer alone
#   sql   the single combined query, an `explode` over an array of per-scenario
#         structs
#
# Two things about scope, stated here because they bound what these numbers mean:
#
#   - The input table is a RECONSTRUCTION from joinless_v5.tex:856-910. The
#     dataset the case study was measured over does not exist here.
#   - `.par` and the case study around it were REMOVED from the manuscript: they
#     appear in v2-v5 and in none of v8, v9, v10. This measures material the
#     paper no longer contains.
#
# Equivalence is checked per scenario by summarize_multiquery.py, not only on the
# total: an unpivot that attached rows to the wrong scenario would still balance
# in aggregate.

set -uo pipefail

DATA="${1:-$HOME/ssb_scenarios}"
RUNS="${RUNS:-5}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
JPP="$HERE/jpp"
OUT="${OUT:-$ROOT/results/multiquery.csv}"
LOGDIR="${LOGDIR:-$ROOT/results/multiquery-logs}"
PLANDIR="${PLANDIR:-$ROOT/results/plans/multiquery}"

mkdir -p "$(dirname "$OUT")" "$LOGDIR" "$PLANDIR"
cd "$ROOT" || exit 1
export JPP_PLAN_DIR="$PLANDIR"

if [[ ! -d "$DATA/scenarios" ]]; then
  echo "no scenario table at $DATA/scenarios" >&2
  echo "generate it first:" >&2
  echo "  ./scripts/jpp GenerateScenarios $DATA" >&2
  exit 1
fi

echo "=============================================================="
echo " scenario unpivot   data=$DATA   runs=$RUNS   $(date)"
echo "=============================================================="

for r in $(seq 1 "$RUNS"); do
  for variant in par seq sql; do
    tag="unpivot-${variant}-r${r}"
    echo "----- $tag -----"
    JPP_RUN_LABEL="$tag" "$JPP" ScenarioUnpivot "$DATA" "$variant" "$OUT" \
      > "$LOGDIR/${tag}.log" 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]; then
      echo "  -> FAILED rc=$rc (see $LOGDIR/${tag}.log)"
    else
      grep -E "^(rows out|elapsed)" "$LOGDIR/${tag}.log" | sed 's/^/  /'
    fi
  done
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " csv:  $OUT"
echo " next: python3 scripts/summarize_multiquery.py $OUT"
echo "=============================================================="
