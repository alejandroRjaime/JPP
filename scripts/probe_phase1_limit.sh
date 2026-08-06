#!/usr/bin/env bash
# Where Phase 1 of the pattern stops working, and whether it is a configuration
# limit or a real one.
#
#   ./scripts/probe_phase1_limit.sh ~/ssb_medium/dim1000mb
#
# Phase 1 collects the whole dimension to the driver before encoding it into the
# lookup array. Two runs are needed to say anything useful about the failure:
#
#   default   spark.driver.maxResultSize at its 1 GB default. A failure here
#             says only that a default was exceeded.
#   relaxed   maxResultSize raised well above the dimension. A failure that
#             survives this is a limit of the implementation, not of a setting,
#             and that distinction is the whole point of running both.
#
# Output: results/phase1_limit_<dir>_{default,relaxed}.{csv,log}

set -uo pipefail

DIM="${1:?usage: probe_phase1_limit.sh <dimensionDir>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
RELAXED="${RELAXED:-8g}"
NAME="$(basename "$DIM")"
OUTDIR="$ROOT/results"
mkdir -p "$OUTDIR"
cd "$ROOT" || exit 1

for mode in default relaxed; do
  csv="$OUTDIR/phase1_limit_${NAME}_${mode}.csv"
  log="$OUTDIR/phase1_limit_${NAME}_${mode}.log"
  echo "===== $NAME / $mode ====="
  if [[ "$mode" == "relaxed" ]]; then
    JPP_EXTRA_CONF="spark.driver.maxResultSize=$RELAXED" \
      "$HERE/jpp" DimStats "$DIM" "$csv" > "$log" 2>&1
  else
    "$HERE/jpp" DimStats "$DIM" "$csv" > "$log" 2>&1
  fi
  echo "  exit=$?"
  grep -E "lookup build failed|^customer|^supplier|^part|^date" "$log" | grep -v "^26/" || true
done

echo
echo "csv/logs under $OUTDIR/phase1_limit_${NAME}_*"
