# Baselines

The comparison in which runtime, hardware, storage and data are held constant and only the
physical execution changes. These are the primary baselines of the paper; the external
engines situate the result, but do not isolate the effect of the pattern.

| File | What it is |
|---|---|
| `SparkBaselines.scala` | Sort-Merge Join and forced Broadcast Hash Join, Q1 only |
| `SparkBaselineQueries.scala` | The same two strategies across the full query set (Q1–Q4) |
| `JoinlessBase.scala` | Base variant of the pattern: broadcast `HashMap` lookups, the 68.10 s configuration |

Both files disable implicit broadcasting, so that the Sort-Merge Join baseline is not
silently converted into a broadcast plan and the Broadcast Hash Join is forced explicitly
by hint.

Projections are kept minimal — each dimension contributes only the columns its query
needs — so that the baselines carry the same payload through the join that the Joinless
implementation materializes. Widening them would make the comparison non-equivalent.

## The base variant

`JoinlessBase.scala` is the pattern without any of the optimizations: dimensions
broadcast as Scala `HashMap`s, probed by hash lookup, folded into a partition-local
`HashMap` keyed by the group attributes. It carries no preconditions — keys may be
sparse or non-numeric and the group-by attributes may be of any type or cardinality —
and it computes the same result as the optimized variant, which is what makes the
optimization-path table meaningful.

Both variants must agree on the group count and the aggregate sum. That check is the
point of publishing them together.
