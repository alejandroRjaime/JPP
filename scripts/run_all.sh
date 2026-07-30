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
    --conf "spark.local.dir=$SPARK_TMP" \
    --conf "spark.driver.extraJavaOptions=$SNAPPY_OPT" \
    --conf "spark.executor.extraJavaOptions=$SNAPPY_OPT" \
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
