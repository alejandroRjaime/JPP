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

# Snappy extracts its native library to a temp directory and maps it executable.
# On systems where /tmp is mounted noexec that mapping fails, and every Parquet
# read or write dies with UnsatisfiedLinkError. Point it somewhere executable.
SNAPPY_TMP="${SNAPPY_TMP:-$HOME/tmp}"
mkdir -p "$SNAPPY_TMP"
SNAPPY_OPT="-Dorg.xerial.snappy.tempdir=$SNAPPY_TMP"
# Spark spills shuffle data to spark.local.dir, which defaults to /tmp. On systems
# where /tmp is a small tmpfs, the Sort-Merge Join baseline exhausts it within
# seconds: it redistributes the fact table once per dimension. Point it at real
# disk with room to spare — budget several times the fact-table size.
SPARK_TMP="${SPARK_TMP:-$HOME/spark-tmp}"
mkdir -p "$SPARK_TMP"

[[ -f "$JAR" ]] || { echo "Build first:  sbt package" >&2; exit 1; }

submit () {
  spark-submit --class "$1" --master "local[$CORES]" \
    --driver-memory "$DRIVER_MEM" \
    --conf spark.sql.adaptive.enabled=false \
    --conf "spark.local.dir=$SPARK_TMP" \
    --conf "spark.driver.extraJavaOptions=$SNAPPY_OPT" \
    --conf "spark.executor.extraJavaOptions=$SNAPPY_OPT" \
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
