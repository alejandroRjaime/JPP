#!/usr/bin/env python3
"""Parse a benchmark log and report the median of each configuration.

    python3 scripts/summarize.py ~/queryset.log

Discards the first run of each configuration as warm-up and reports the median
of the rest, which is the protocol described in the paper. Also checks that
every system agrees on the group count and the aggregate sum for a given query.
"""
import re, sys, statistics as st
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "queryset.log"
num  = lambda s: float(s.replace(".", "").replace(",", ".")) if "," in s else float(s)

# A record is flushed on `elapsed`, which every runner prints as the last line of
# its block. Keying on `query` instead would silently drop any runner that does
# not print one — JoinlessBase prints `variant` — folding its fields into the
# preceding record and corrupting that baseline as well as losing its own.
runs, cur, unlabelled = [], {}, 0
for line in open(path, encoding="utf-8", errors="ignore"):
    m = re.match(r"^\s*(query|strategy|variant|groups|total \(check\)|elapsed)\s*=\s*(.+?)\s*$", line)
    if not m:
        continue
    k, v = m.group(1), m.group(2)
    if k == "query":
        cur["query"] = v
    elif k in ("strategy", "variant"):
        cur["strategy"] = v
    elif k == "groups":
        cur["groups"] = int(v)
    elif k == "total (check)":
        cur["total"] = num(v)
    elif k == "elapsed":
        cur["elapsed"] = num(v.replace(" s", ""))
        if "query" not in cur:
            unlabelled += 1
        cur.setdefault("query", "UNLABELLED")
        cur.setdefault("strategy", "joinless")
        runs.append(cur)
        cur = {}

if unlabelled:
    print(f"WARNING: {unlabelled} run(s) printed no 'query =' line and are grouped "
          f"under UNLABELLED. Fix the runner rather than guessing which query it was.\n")

by = defaultdict(list)
for r in runs:
    if "elapsed" in r:
        by[(r["query"], r["strategy"])].append(r)

print(f"{len(runs)} runs parsed\n")

# --- equivalence check ---------------------------------------------------
print("=== functional equivalence ===")
for q in sorted({r["query"] for r in runs}):
    sigs = {(r.get("groups"), round(r.get("total", 0), 1)) for r in runs if r["query"] == q}
    flag = "OK " if len(sigs) == 1 else "MISMATCH"
    print(f"{flag} {q}: " + " | ".join(f"{g} groups, sum {t:,.1f}" for g, t in sorted(sigs)))

# --- medians -------------------------------------------------------------
print("\n=== medians (first run of each configuration discarded) ===")
print(f"{'query':6} {'strategy':10} {'n':>3} {'median':>9} {'min':>9} {'max':>9}")
table = {}
for (q, s), rs in sorted(by.items()):
    times = [r["elapsed"] for r in rs]
    warm  = times[1:] if len(times) > 1 else times
    med   = st.median(warm)
    table[(q, s)] = med
    print(f"{q:6} {s:10} {len(warm):3d} {med:9.2f} {min(warm):9.2f} {max(warm):9.2f}")

# --- ratios --------------------------------------------------------------
print("\n=== ratio vs joinless ===")
for q in sorted({q for q, _ in table}):
    base = table.get((q, "joinless"))
    if not base:
        continue
    parts = [f"{s}: {table[(q,s)]/base:.2f}x" for _, s in sorted(table) if _ == q and s != "joinless"]
    print(f"{q}: joinless {base:.2f}s   " + "   ".join(parts))
