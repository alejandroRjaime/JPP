#!/usr/bin/env bash
# Runs every Spark configuration on the same dataset and prints the timings.
#
# Usage:  ./scripts/run_all.sh ~/ssb_synth
#
# Each configuration is executed five times; discard the first as warm-up and
# take the median of the remaining four, as reported in the paper.

set -euo pipefail

DATA_DIR="${1:-$HOME/ssb_synth}"
JAR="target/scala-2.12/jpp_2.12-1.0.jar"
RUNS="${RUNS:-5}"
CORES="${CORES:-16}"
DRIVER_MEM="${DRIVER_MEM:-32g}"

if [[ ! -f "$JAR" ]]; then
  echo "Build first:  sbt package" >&2
  exit 1
fi

submit () {
  local class="$1"; shift
  spark-submit \
    --class "$class" \
    --master "local[$CORES]" \
    --driver-memory "$DRIVER_MEM" \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.sql.shuffle.partitions=64 \
    "$JAR" "$DATA_DIR" "$@"
}

for run in $(seq 1 "$RUNS"); do
  echo "===== run $run/$RUNS ====="
  submit JoinlessPartitionPattern            # optimized variant
  submit JoinlessBase                        # base HashMap variant
  submit SparkBaselines "$DATA_DIR" smj      # Sort-Merge Join
  submit SparkBaselines "$DATA_DIR" bhj      # Broadcast Hash Join
done

echo
echo "Every configuration must report 175 groups and a check sum of 3000078095504.6."
echo "A configuration that does not is computing something else and is not a baseline."
