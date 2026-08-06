#!/usr/bin/env bash
# The threshold sweep repeated over dimensions in the 10 MB - 1 GB range the
# paper motivates the pattern over.
#
#   ./scripts/run_threshold_sweep_medium.sh ~/ssb_medium ~/ssb_synth 2>&1 | tee ~/sweep_medium.log
#
# Reuses run_threshold_sweep.sh unchanged, once per generated dimension size, so
# the two sets of numbers come from the same code path and differ only in the
# data they were taken over. Each size writes its own dimension_sizes.csv, which
# is what carries the lookup-bytes-versus-raw-bytes comparison for that size.
#
# Sizes are discovered from the directory rather than hard-coded, so the set
# measured is whatever GenerateMediumDims was asked to produce.

set -uo pipefail

MEDIUM="${1:-$HOME/ssb_medium}"
ORIGINAL="${2:-$HOME/ssb_synth}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
RUNS="${RUNS:-5}"
QUERIES="${QUERIES:-q1}"
OUT="${OUT:-$ROOT/results/threshold_sweep_medium.csv}"

if [[ ! -d "$MEDIUM" ]]; then
  echo "no such directory: $MEDIUM" >&2
  echo "generate it first:" >&2
  echo "  ./scripts/jpp GenerateMediumDims $MEDIUM 10,100,1000 $ORIGINAL" >&2
  exit 1
fi

# SIZES restricts which generated sizes are swept, as a space-separated list of
# directory names. Without it every size found is swept, which is right for a
# first pass but wasteful when a later question only concerns some of them — the
# 1 GB point costs an hour and its pattern runs are already known to fail.
mapfile -t DIRS < <(find "$MEDIUM" -maxdepth 1 -type d -name 'dim*mb' | sort -V)
if [[ -n "${SIZES:-}" ]]; then
  filtered=()
  for d in "${DIRS[@]}"; do
    for want in $SIZES; do
      [[ "$(basename "$d")" == "$want" ]] && filtered+=("$d")
    done
  done
  DIRS=("${filtered[@]}")
fi
if [[ ${#DIRS[@]} -eq 0 ]]; then
  echo "no dim*mb directories under $MEDIUM" >&2
  exit 1
fi

echo "=============================================================="
echo " medium-dimension sweep   sizes=${#DIRS[@]}   runs=$RUNS   $(date)"
echo "=============================================================="

# One header for the combined CSV; a `dimension_set` column distinguishes the
# sizes, which the per-size runs cannot supply themselves.
if [[ ! -s "$OUT" ]]; then
  echo "dimension_set,query,strategy,threshold_bytes,threshold_label,run,status,elapsed_s,setup_s,groups,total,bhj,smj,shuffle,bcast_ex,exchanges,bcast_ms,bcast_bytes,plan_file" > "$OUT"
fi

for d in "${DIRS[@]}"; do
  name="$(basename "$d")"
  echo
  echo "########## $name ##########"

  if [[ ! -e "$d/lineorder" ]]; then
    echo "SKIP $name: no lineorder (symlink it to $ORIGINAL/lineorder)" >&2
    continue
  fi

  tmp="$ROOT/results/_sweep_${name}.csv"
  rm -f "$tmp"

  RUNS="$RUNS" QUERIES="$QUERIES" \
  OUT="$tmp" \
  DIMCSV="$ROOT/results/dimension_sizes_${name}.csv" \
  PLANDIR="$ROOT/results/plans/${name}" \
  LOGDIR="$ROOT/results/sweep-logs/${name}" \
  FORCE_DIMSTATS=1 \
    "$HERE/run_threshold_sweep.sh" "$d"

  # Prefix every data row with the size label and fold into the combined CSV.
  if [[ -s "$tmp" ]]; then
    tail -n +2 "$tmp" | sed "s|^|${name},|" >> "$OUT"
    rm -f "$tmp"
  fi
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " csv:  $OUT"
echo " per-size dimension sizes: $ROOT/results/dimension_sizes_dim*mb.csv"
echo "=============================================================="
