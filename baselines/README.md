# Baselines

The comparison in which runtime, hardware, storage and data are held constant and only the
physical execution changes. These are the primary baselines of the paper; the external
engines situate the result, but do not isolate the effect of the pattern.

| File | What it is |
|---|---|
| `SparkBaselines.scala` | Sort-Merge Join and forced Broadcast Hash Join, Q1 only |
| `SparkBaselineQueries.scala` | The same two strategies across the full query set (Q1–Q4) |
| `JoinlessBase.scala` | **Not yet added.** See below. |

Both files disable implicit broadcasting, so that the Sort-Merge Join baseline is not
silently converted into a broadcast plan and the Broadcast Hash Join is forced explicitly
by hint.

Projections are kept minimal — each dimension contributes only the columns its query
needs — so that the baselines carry the same payload through the join that the Joinless
implementation materializes. Widening them would make the comparison non-equivalent.

## Missing: `JoinlessBase.scala`

The base variant of the pattern: dimensions broadcast as Scala `HashMap`s, looked up
inside `mapPartitions`, aggregated locally. This is Listing 1 of the paper and the
68.10 s row of the optimization-path table. It belongs here alongside the optimized
variant so that both regimes reported in the paper can be reproduced.
