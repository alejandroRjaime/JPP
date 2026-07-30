#!/usr/bin/env bash
# Runs the four-query set across every Spark configuration.
#
#   ./scripts/run_queryset.sh ~/ssb_synth
#
# Five runs each; discard the first, take the median of the remaining four.
# Before comparing any timing, check that all systems agree on the group count
# and the aggregate sum for each query.

set -euo pipefail

DATA_DIR="${1:-$HOME/ssb_synth}"
JAR="target/scala-2.12/jpp_2.12-1.0.jar"
RUNS="${RUNS:-5}"
CORES="${CORES:-16}"
DRIVER_MEM="${DRIVER_MEM:-32g}"

[[ -f "$JAR" ]] || { echo "Build first:  sbt package" >&2; exit 1; }

submit () {
  spark-submit --class "$1" --master "local[$CORES]" \
    --driver-memory "$DRIVER_MEM" \
    --conf spark.sql.adaptive.enabled=false \
    "$JAR" "${@:2}"
}

for q in q1 q2 q3 q4; do
  for run in $(seq 1 "$RUNS"); do
    echo "===== $q  run $run/$RUNS ====="
    submit JoinlessQueries      "$DATA_DIR" "$q"
    submit SparkBaselineQueries "$DATA_DIR" "$q" smj
    submit SparkBaselineQueries "$DATA_DIR" "$q" bhj
  done
done
