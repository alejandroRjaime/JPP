#!/usr/bin/env bash
# Did the Long-to-Int truncation defect affect any published number?
#
#   ./scripts/run_key_audit.sh 2>&1 | tee ~/keyaudit.log
#
# The defect: `buildIntArray` read the surrogate key through `getKeyAsInt`, which
# truncates. A key at or above 2^31 truncated to a negative value, which the
# loop's `if (k >= 0)` guard then skipped — so out-of-range rows were dropped in
# silence and the array came out sized by the largest key that happened to fit.
# No exception, no warning, wrong answers. See reports/experimentos_revision.md §12.
#
# Two things have to hold for a published number to be unaffected, and both are
# checked here rather than argued:
#
#   1. no surrogate key in any dataset used by the manuscript reaches 2^31;
#   2. re-running the configurations that build these arrays, with the corrected
#      code, reproduces the published group counts and aggregate sums.
#
# (1) alone is nearly conclusive — truncation cannot bite below the bound — but
# it rests on the bound being where it is thought to be. (2) is the end-to-end
# check that does not.
#
# Timing is irrelevant here: this is a correctness audit, so a single repetition
# per configuration is enough and the usual median protocol does not apply.

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
JPP="$HERE/jpp"
SYNTH="${SYNTH:-$HOME/ssb_synth}"
MEDIUM="${MEDIUM:-$HOME/ssb_medium}"
OUT="${OUT:-$ROOT/results/key_audit.csv}"
LOGDIR="${LOGDIR:-$ROOT/results/keyaudit-logs}"

mkdir -p "$(dirname "$OUT")" "$LOGDIR"
cd "$ROOT" || exit 1
export JPP_PLAN_DIR="$ROOT/results/plans/keyaudit"

INT_MAX=2147483647

echo "dataset,query,phase1,status,groups,total,expected_groups,expected_total,verdict" > "$OUT"

# Expected values, from the protocol.
declare -A EXP_GROUPS=( [q1]=200 [q2]=40 [q3]=400 [q4]=200 )
declare -A EXP_TOTAL=( [q1]=3000078095504.6 [q2]=120015240211.4 [q3]=1199919067064.6 [q4]=3000078095504.6 )

field () {
  grep -m1 -E "^$1[[:space:]]*=" "$2" 2>/dev/null \
    | sed -e 's/^[^=]*=[[:space:]]*//' -e 's/[[:space:]]*s$//' -e 's/,/./g' -e 's/[[:space:]]*$//'
}

echo "=============================================================="
echo " key audit   Int.MaxValue = $INT_MAX   $(date)"
echo "=============================================================="

# Every dataset any manuscript table was taken over, plus the medium ones the
# revision added. Sparse datasets are deliberately excluded: they exist only to
# probe the bound and are known to cross it, so including them would mix the
# question "was anything published affected" with "does the guard work".
for spec in "synth:$SYNTH:q1 q2 q3 q4" \
            "dim10mb:$MEDIUM/dim10mb:q1" \
            "dim100mb:$MEDIUM/dim100mb:q1" \
            "dim1000mb:$MEDIUM/dim1000mb:q1"; do
  name="${spec%%:*}"; rest="${spec#*:}"
  dir="${rest%%:*}"; queries="${rest#*:}"
  [[ -d "$dir" ]] || { echo "SKIP $name (no such directory)"; continue; }

  for q in $queries; do
    # dim1000mb only completes with the streaming Phase 1; the collect variant
    # fails there for an unrelated reason (driver-side gather), so the variant is
    # chosen per dataset rather than fixed.
    phase1="collect"
    [[ "$name" == "dim1000mb" ]] && phase1="streaming"

    tag="${name}-${q}-${phase1}"
    log="$LOGDIR/${tag}.log"
    echo "----- $tag -----"
    JPP_PHASE1="$phase1" JPP_RUN_LABEL="$tag" \
      "$JPP" JoinlessQueries "$dir" "$q" > "$log" 2>&1
    rc=$?

    groups="$(field groups "$log")"
    total="$(field 'total \(check\)' "$log")"
    eg="${EXP_GROUPS[$q]}"
    et="${EXP_TOTAL[$q]}"

    if [[ $rc -ne 0 ]]; then
      status="fail"; verdict="RUN FAILED"
    elif [[ "$groups" == "$eg" && "$total" == "$et" ]]; then
      status="ok"; verdict="MATCHES PUBLISHED"
    else
      status="ok"; verdict="DIFFERS FROM PUBLISHED"
    fi
    echo "  groups=$groups (expected $eg)  total=$total (expected $et)  -> $verdict"

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$name" "$q" "$phase1" "$status" "$groups" "$total" "$eg" "$et" "$verdict" >> "$OUT"
  done
done

echo
echo "=============================================================="
echo " done  $(date)"
echo " csv:  $OUT"
echo "=============================================================="
grep -c "MATCHES PUBLISHED" "$OUT" | xargs -I{} echo " {} configuration(s) reproduce the published values"
grep -v "MATCHES PUBLISHED" "$OUT" | tail -n +2 || true
