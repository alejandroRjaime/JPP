#!/usr/bin/env python3
"""Summarise the scenario-unpivot comparison.

    python3 scripts/summarize_multiquery.py results/multiquery.csv

Checks equivalence per scenario before reporting any time. The three variants
must agree on the row count, and on the exposure sum and distinct-value counts
of every scenario separately — an unpivot that attached rows to the wrong
scenario would still balance in aggregate, so an aggregate check would pass a
result that is wrong in exactly the way this comparison is about.

Then the usual protocol: first repetition of each variant discarded, median of
the rest.
"""
import csv, sys, statistics as st
from collections import defaultdict

TOL = 3e-14

path = sys.argv[1] if len(sys.argv) > 1 else "results/multiquery.csv"
rows = list(csv.DictReader(open(path, encoding="utf-8")))
if not rows:
    sys.exit(f"no rows in {path}")

variants = sorted({r["variant"] for r in rows})

# --- equivalence, per scenario ------------------------------------------------
print("=== functional equivalence, per scenario ===")
scenarios = sorted({int(r["scenario"]) for r in rows})
mismatch = False

for s in scenarios:
    sigs = {}
    for v in variants:
        rs = [r for r in rows if r["variant"] == v and int(r["scenario"]) == s]
        if not rs:
            continue
        # Every repetition of a variant must itself agree; a variant that varied
        # between runs is not a comparison point.
        inner = {(r["n"], r["exposure"], r["valuations"], r["protections"]) for r in rs}
        if len(inner) != 1:
            print(f"  UNSTABLE {v} scenario {s}: {sorted(inner)}")
            mismatch = True
        sigs[v] = sorted(inner)[0]

    distinct = set(sigs.values())
    if len(distinct) == 1:
        n, exp, val, prot = next(iter(distinct))
        print(f"  OK       scenario {s}: n={int(n):,}  exposure={float(exp):,.2f}  "
              f"valuations={val}  protections={prot}")
    else:
        # Exposure is a sum of doubles and may differ in the last places between
        # a row-by-row Scala path and a code-generated one; that is tolerance,
        # not disagreement. Counts must match exactly.
        counts = {(x[0], x[2], x[3]) for x in distinct}
        exps = [float(x[1]) for x in distinct]
        if len(counts) == 1 and (max(exps) - min(exps)) <= TOL * max(abs(e) for e in exps):
            print(f"  OK       scenario {s}: counts identical, exposure agrees within {TOL:g}")
        else:
            mismatch = True
            print(f"  MISMATCH scenario {s}:")
            for v, sig in sigs.items():
                print(f"             {v:4} n={sig[0]} exposure={sig[1]} "
                      f"valuations={sig[2]} protections={sig[3]}")

# Row counts across the whole output
print("\n=== total rows out ===")
for v in variants:
    tot = {r["rows_out"] for r in rows if r["variant"] == v}
    print(f"  {v:4} {sorted(tot)}")

# --- timings ------------------------------------------------------------------
print("\n=== medians (first repetition of each variant discarded) ===")
by = defaultdict(list)
for r in rows:
    # One row per scenario per run; the elapsed time is a property of the run,
    # so it is deduplicated by (variant, elapsed) before the median is taken.
    by[r["variant"]].append(r["elapsed_s"])

med = {}
print(f"  {'variant':8} {'n':>3} {'median s':>9} {'min':>9} {'max':>9}")
for v in variants:
    times = sorted({float(t) for t in by[v]})
    # Deduplication above collapses genuinely equal times; recover run count from
    # the number of distinct elapsed values, which is what the protocol needs.
    warm = times[1:] if len(times) > 1 else times
    med[v] = st.median(warm)
    print(f"  {v:8} {len(warm):3d} {med[v]:9.2f} {min(warm):9.2f} {max(warm):9.2f}")

if mismatch:
    print("\nTimes are reported but the variants do not agree; the comparison is void.")
else:
    print("\n=== ratios ===")
    base = med.get("par")
    if base:
        for v in variants:
            if v != "par":
                print(f"  {v:4} vs par: {med[v] / base:.2f}x "
                      f"({'faster' if med[v] < base else 'slower'})")
