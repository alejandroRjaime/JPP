#!/usr/bin/env python3
"""Shuffle volume per strategy, and the network time that volume would cost.

    python3 scripts/summarize_shuffle.py results/shuffle_bytes.csv

Volumes are MEASURED, from Spark's task metrics. The transfer times are a
PROJECTION and are labelled as such wherever they appear. What separates the two
is stated in full below rather than in a footnote, because the distinction is
the whole value of the exercise: the manuscript already says the wall-clock
effect of moving shuffle onto a network "remains an empirical question"
(joinless_v10_Jul_31.tex:1264), and a projection presented as a measurement
would answer that question falsely.

The projection assumes:

  - every shuffle byte crosses the link. With E executors roughly 1/E of reads
    would be node-local, so at E=2 about half of this volume would stay local
    and the projected time is correspondingly high;
  - the link runs at the effective rates the paper measured, 88% to 91% of the
    125 MB/s line rate (:1276-1277, :1933);
  - transfer does not overlap with compute.

It excludes, and therefore under-counts: broadcast, coordination, skew,
scheduling and scaling overheads — the five the manuscript itself lists at :1264.

Neither a strict upper nor a strict lower bound, then. It is a scale indicator
for a cost the single-node measurement does not charge at all.
"""
import csv, sys, statistics as st
from collections import defaultdict

LINE_RATE_MB_S = 125.0
EFF_LOW, EFF_HIGH = 0.88, 0.91

path = sys.argv[1] if len(sys.argv) > 1 else "results/shuffle_bytes.csv"
rows = [r for r in csv.DictReader(open(path, encoding="utf-8")) if r["status"] == "ok"]
if not rows:
    sys.exit(f"no usable rows in {path}")


def num(v, cast=float):
    try:
        return cast(v)
    except (TypeError, ValueError):
        return None


# --- equivalence, before anything is compared --------------------------------
print("=== functional equivalence ===")
bad = False
for q in sorted({r["query"] for r in rows}):
    sig = {(r["groups"], r["total"]) for r in rows if r["query"] == q}
    if len(sig) == 1:
        g, t = next(iter(sig))
        print(f"  OK       {q}: {g} groups, sum {t}")
    else:
        bad = True
        print(f"  MISMATCH {q}: {sorted(sig)}")
if bad:
    print("\nVolumes are reported but the configurations disagree; the comparison is void.\n")

# --- measured volumes ---------------------------------------------------------
by = defaultdict(list)
for r in rows:
    by[(r["query"], r["strategy"])].append(r)

print("\n=== MEASURED shuffle volume (median of repetitions after the first) ===")
hdr = (f"  {'query':6} {'strategy':9} {'n':>2} {'write bytes':>16} {'write records':>15} "
       f"{'remote read':>12} {'elapsed s':>10}")
print(hdr)
print("  " + "-" * (len(hdr) - 2))

med = {}
for k in sorted(by):
    rs = by[k]
    w = rs[1:] if len(rs) > 1 else rs
    def m(col, cast=float):
        vals = [num(r[col], cast) for r in w]
        vals = [v for v in vals if v is not None]
        return st.median(vals) if vals else None
    wb, wr = m("shuffle_write_bytes"), m("shuffle_write_records")
    rr, el = m("shuffle_read_remote"), m("elapsed_s")
    med[k] = (wb, wr, el)
    print(f"  {k[0]:6} {k[1]:9} {len(w):2d} {int(wb):16,d} {int(wr):15,d} "
          f"{int(rr):12,d} {el:10.2f}")

print("\n  Remote read is zero everywhere: on a single node every shuffle read is")
print("  local. That is the finding — the whole of this volume is what would")
print("  become network traffic in a distributed deployment.")

# --- projection ---------------------------------------------------------------
print("\n=== PROJECTION — not a measurement ===")
print(f"  Link 1 Gbit = {LINE_RATE_MB_S:.0f} MB/s line rate; effective "
      f"{EFF_LOW:.0%}-{EFF_HIGH:.0%} as measured in the paper (:1276-1277, :1933),")
print(f"  i.e. {LINE_RATE_MB_S * EFF_LOW:.1f}-{LINE_RATE_MB_S * EFF_HIGH:.2f} MB/s.")
print("  Assumes every shuffle byte crosses the link and that transfer does not")
print("  overlap compute. Excludes broadcast, coordination, skew, scheduling and")
print("  scaling — the five overheads the manuscript lists at :1264.\n")

hdr2 = (f"  {'query':6} {'strategy':9} {'write GB':>10} {'proj. s @91%':>13} "
        f"{'proj. s @88%':>13} {'measured s':>11} {'proj/measured':>14}")
print(hdr2)
print("  " + "-" * (len(hdr2) - 2))
for k in sorted(med):
    wb, _, el = med[k]
    gb = wb / 1e9
    hi = wb / (LINE_RATE_MB_S * EFF_HIGH * 1e6)
    lo = wb / (LINE_RATE_MB_S * EFF_LOW * 1e6)
    ratio = hi / el if el else float("nan")
    print(f"  {k[0]:6} {k[1]:9} {gb:10.3f} {hi:13.1f} {lo:13.1f} {el:11.2f} {ratio:13.1f}x")

# --- ratios between strategies -------------------------------------------------
print("\n=== shuffle volume relative to the pattern ===")
for q in sorted({k[0] for k in med}):
    base = med.get((q, "joinless"), (None,))[0]
    if not base:
        continue
    parts = []
    for s in ("bhj", "smj"):
        v = med.get((q, s), (None,))[0]
        if v:
            parts.append(f"{s}: {v / base:,.0f}x")
    print(f"  {q}: pattern {base / 1e6:,.2f} MB   " + "   ".join(parts))
