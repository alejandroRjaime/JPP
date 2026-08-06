#!/usr/bin/env python3
"""Summarise a threshold sweep into tables ready to become paper tables.

    python3 scripts/summarize_sweep.py results/threshold_sweep.csv

Applies the same protocol as summarize.py — first repetition of each
configuration discarded as warm-up, median of the rest — and additionally
reports, per point, the plan Spark chose and the broadcast cost it paid.

A time is only printed for a configuration whose runs all agree on the group
count and the aggregate sum. A configuration that computed something else is
not a comparison point, so its time is suppressed rather than shown with a
footnote.
"""
import csv, sys, statistics as st
from collections import defaultdict

TOL = 3e-14  # relative tolerance of the functional-equivalence check

path = sys.argv[1] if len(sys.argv) > 1 else "results/threshold_sweep.csv"
rows = list(csv.DictReader(open(path, encoding="utf-8")))
if not rows:
    sys.exit(f"no rows in {path}")


def f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def i(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


ok      = [r for r in rows if r["status"] == "ok"]
failed  = [r for r in rows if r["status"] != "ok"]

# The medium-dimension sweep prefixes every row with the dimension set it was
# taken over, and runs the same query over each. Without folding that column
# into the grouping key every size would collapse into one point and the medians
# would silently mix three different experiments.
HAS_SET = "dimension_set" in rows[0]


def qkey(r):
    return f"{r['dimension_set']}/{r['query']}" if HAS_SET else r["query"]

# --- functional equivalence, per query ---------------------------------------
print("=== functional equivalence ===")
mismatched = set()
for q in sorted({qkey(r) for r in ok}):
    sigs = {}
    for r in (x for x in ok if qkey(x) == q):
        g, t = i(r["groups"]), f(r["total"])
        if g is None or t is None:
            continue
        sigs.setdefault((g, round(t, 1)), []).append(r)
    if not sigs:
        print(f"  {q}: no usable rows")
        continue
    if len(sigs) == 1:
        (g, t), _ = next(iter(sigs.items()))
        print(f"  OK       {q}: {g} groups, sum {t:,.1f}")
    else:
        # Same group count with sums differing only within tolerance is
        # agreement, not a mismatch: the pattern and a join plan sum the same
        # doubles in a different order.
        groups = {g for g, _ in sigs}
        totals = [t for _, t in sigs]
        if len(groups) == 1 and (max(totals) - min(totals)) <= TOL * max(abs(x) for x in totals):
            print(f"  OK       {q}: {groups.pop()} groups, sums agree within {TOL:g}")
        else:
            mismatched.add(q)
            print(f"  MISMATCH {q}: " + " | ".join(f"{g} groups, sum {t:,.1f}" for g, t in sorted(sigs)))

# --- medians per configuration -----------------------------------------------
by = defaultdict(list)
for r in ok:
    if f(r["elapsed_s"]) is not None:
        by[(qkey(r), r["strategy"], r["threshold_label"])].append(r)


def median_elapsed(rs):
    times = [f(r["elapsed_s"]) for r in rs]
    times = [t for t in times if t is not None]
    warm = times[1:] if len(times) > 1 else times
    return st.median(warm), len(warm)


print("\n=== per-point results (first repetition discarded) ===")
hdr = (f"{'query':16} {'strategy':9} {'threshold':30} {'n':>2} {'median s':>9} "
       f"{'bhj':>4} {'smj':>4} {'shuf':>5} {'bcEx':>5} {'bcast ms':>9}")
print(hdr)
print("-" * len(hdr))

table = {}
for key in sorted(by, key=lambda k: (k[0], k[1] != "joinless", k[2])):
    q, s, lab = key
    rs = by[key]
    med, n = median_elapsed(rs)
    # Plan shape is a property of the configuration, not of the repetition; if
    # the repetitions disagree the point is reported as varying rather than
    # averaged, because averaging plans is meaningless.
    def uniq(col):
        vals = {i(r[col]) for r in rs if i(r[col]) is not None}
        return str(vals.pop()) if len(vals) == 1 else ("~" if not vals else "varies")
    bc = [i(r["bcast_ms"]) for r in rs if i(r["bcast_ms"]) is not None]
    bcm = st.median(bc[1:] if len(bc) > 1 else bc) if bc else None
    table[key] = (med, uniq("bhj"), uniq("smj"), uniq("shuffle"), uniq("bcast_ex"), bcm)
    flag = "  [MISMATCH]" if q in mismatched else ""
    print(f"{q:16} {s:9} {lab:30} {n:2d} {med:9.2f} "
          f"{uniq('bhj'):>4} {uniq('smj'):>4} {uniq('shuffle'):>5} {uniq('bcast_ex'):>5} "
          f"{(f'{bcm:.0f}' if bcm is not None else '-'):>9}{flag}")

# --- ratio against the pattern ------------------------------------------------
print("\n=== relational plan vs the pattern (median s, ratio) ===")
for q in sorted({k[0] for k in table}):
    base = next((v[0] for k, v in table.items() if k[0] == q and k[1] == "joinless"), None)
    if base is None:
        print(f"  {q}: no joinless reference point")
        continue
    print(f"  {q}: pattern {base:.2f}s")
    for k in sorted(k for k in table if k[0] == q and k[1] != "joinless"):
        v = table[k][0]
        print(f"      {k[2]:28} {v:8.2f}s  {v / base:5.2f}x")

# --- failures -----------------------------------------------------------------
print("\n=== failures (these are results, not omissions) ===")
if not failed:
    print("  none")
else:
    agg = defaultdict(int)
    for r in failed:
        agg[(qkey(r), r["threshold_label"], r["status"])] += 1
    for (q, lab, stt), n in sorted(agg.items()):
        print(f"  {q:5} {lab:30} {stt:22} x{n}")

# --- markdown ------------------------------------------------------------------
print("\n=== markdown ===\n")
print("| Query | Configuration | Median (s) | vs pattern | BHJ | SMJ | Shuffle | Broadcast (ms) |")
print("|---|---|---:|---:|---:|---:|---:|---:|")
for q in sorted({k[0] for k in table}):
    base = next((v[0] for k, v in table.items() if k[0] == q and k[1] == "joinless"), None)
    for k in sorted((k for k in table if k[0] == q), key=lambda k: (k[1] != "joinless", k[2])):
        med, bhj, smj, shuf, bcex, bcm = table[k]
        name = "Joinless Partition Pattern" if k[1] == "joinless" else f"auto, {k[2]}"
        ratio = f"{med / base:.2f}x" if base else "—"
        print(f"| {q} | {name} | {med:.2f} | {ratio} | {bhj} | {smj} | {shuf} | "
              f"{bcm:.0f} |" if bcm is not None else
              f"| {q} | {name} | {med:.2f} | {ratio} | {bhj} | {smj} | {shuf} | — |")
