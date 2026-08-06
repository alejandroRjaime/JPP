#!/usr/bin/env bash
# How far the indexed-array lookup survives a sparse key space.
#
#   ./scripts/run_sparse_keys.sh ~/ssb_sparse 2>&1 | tee ~/sparse.log
#
# The array is sized by the LARGEST surrogate key, not by the row count, so a
# dimension with the same number of rows spread over a wider key range costs
# proportionally more memory while occupying the same bytes on disk. Dense keys
# are the stated precondition of this optimization in the paper; this measures
# what happens on the other side of it, and where the boundary actually is.
#
# Two boundaries are expected and are measured rather than argued:
#   - a memory boundary, where the array stops fitting usefully;
#   - a hard addressing boundary at Int.MaxValue = 2,147,483,647, beyond which
#     an Int-indexed array cannot represent the key at all.
#
# Sizes come from DimStats, which reports the structure as measured when it can
# be built and as predicted when it cannot, labelled either way. Timings come
# from run_phase1_variants.sh, so the sparse case is measured by exactly the
# same harness as the dense one.

set -uo pipefail

SPARSE="${1:-$HOME/ssb_sparse}"
RUNS="${RUNS:-5}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
JPP="$HERE/jpp"
OUT="${OUT:-$ROOT/results/dimension_sizes_sparse.csv}"

mkdir -p "$(dirname "$OUT")"
cd "$ROOT" || exit 1

# PATTERN selects which generated sets to measure. The scattered variants are
# written as sparse<f>xs and are deliberately a different set from the tip ones,
# so both constructions can be compared side by side.
PATTERN="${PATTERN:-sparse*x}"
mapfile -t DIRS < <(find "$SPARSE" -maxdepth 1 -type d -name "$PATTERN" \
  | sed 's/.*sparse\([0-9]*\)xs\?/\1 &/' | sort -n | cut -d' ' -f2)

if [[ ${#DIRS[@]} -eq 0 ]]; then
  echo "no sparse*x directories under $SPARSE" >&2
  echo "generate them first:" >&2
  echo "  ./scripts/jpp GenerateSparseDims $SPARSE 10,100,500,700,1000 ~/ssb_synth/lineorder" >&2
  exit 1
fi

# --- 1. sizes ----------------------------------------------------------------
# Each spread factor writes its own CSV; they are concatenated into one file with
# a `spread` column, which the per-directory runs cannot supply themselves.
echo "spread,dimension,rows,disk_bytes,est_full_bytes,est_projected_bytes,lookup_bytes,lookup_bytes_predicted,lookup_status,lookup_elements,max_key,lookup_over_disk,lookup_over_est_projected" > "$OUT"

for d in "${DIRS[@]}"; do
  name="$(basename "$d")"
  factor="${name#sparse}"; factor="${factor%xs}"; factor="${factor%x}"
  tmp="$ROOT/results/_dimsizes_${name}.csv"
  echo "===== sizes: $name ====="
  # The streaming Phase 1 is used here because it checks the Int bound explicitly
  # and reports a clean failure. The collect variant used to truncate the key and
  # silently build an array sized by whatever fitted; that is fixed, but the
  # streaming path is the one that states the limit.
  JPP_PHASE1=streaming "$JPP" DimStats "$d" "$tmp" 2>&1 \
    | grep -E "^(customer|supplier|part|date|\[)" || true
  if [[ -s "$tmp" ]]; then
    tail -n +2 "$tmp" | sed "s/^/${factor},/" >> "$OUT"
    rm -f "$tmp"
  fi
done

echo
echo "sizes written to $OUT"

# --- 2. timings --------------------------------------------------------------
# Reuses the Phase 1 harness so the sparse and dense measurements come from the
# same code path and are directly comparable.
echo
if [[ -n "${SKIP_TIMINGS:-}" ]]; then
  echo "===== timings skipped ====="
  echo "The scattered distribution moves the keys the fact table references, so most"
  echo "fact rows no longer match and the queries do not return the published answer."
  echo "Sizes stay comparable — they do not depend on running a query — but timings"
  echo "do not, so none are taken."
  echo
  echo " sizes: $OUT"
  exit 0
fi
echo "===== timings ====="
SIZES="$(for d in "${DIRS[@]}"; do basename "$d"; done | tr '\n' ' ')" \
RUNS="$RUNS" \
OUT="$ROOT/results/phase1_variants_sparse.csv" \
LOGDIR="$ROOT/results/sparse-logs" \
PLANDIR="$ROOT/results/plans/sparse" \
  "$HERE/run_phase1_variants.sh" "$SPARSE"

echo
echo "=============================================================="
echo " done  $(date)"
echo " sizes:   $OUT"
echo " timings: $ROOT/results/phase1_variants_sparse.csv"
echo "=============================================================="
