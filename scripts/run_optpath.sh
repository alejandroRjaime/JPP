#!/usr/bin/env bash
# Optimization-path table: the base variant of the pattern against the optimized
# variant, over Q1, on identical data and runtime.
#
#   ./scripts/run_optpath.sh ~/ssb_synth 2>&1 | tee ~/optpath.log
#   python3 scripts/summarize.py ~/optpath.log
#
# Exists because the table was not regenerable from the repository: the base
# variant printed no `query =` line, so summarize.py folded its runs into the
# preceding record instead of reporting them. Both are fixed; this script is the
# command that produces the table.
#
# The two variants are interleaved rather than run in blocks. The host is not
# dedicated, so a block layout would let a burst of unrelated load land entirely
# on one variant and be indistinguishable from a property of that variant.

set -uo pipefail

DATA="${1:-$HOME/ssb_synth}"
RUNS="${RUNS:-5}"
HERE="$(cd "$(dirname "$0")" && pwd)"
JPP="$HERE/jpp"

echo "=============================================================="
echo " optimization path   data=$DATA   runs=$RUNS   $(date)"
echo "=============================================================="

for r in $(seq 1 "$RUNS"); do
  echo "----- optimized  run $r/$RUNS -----"
  "$JPP" JoinlessQueries "$DATA" q1
  echo "----- base       run $r/$RUNS -----"
  "$JPP" JoinlessBase    "$DATA"
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " next:  python3 scripts/summarize.py <this log>"
echo "=============================================================="
