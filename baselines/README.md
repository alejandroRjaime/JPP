# Baselines

`SparkBaselines.scala` — Sort-Merge Join and forced Broadcast Hash Join.

`JoinlessBase.scala` — **TO ADD.** The base variant of the pattern: broadcast the
dimensions as Scala `HashMap`s, look them up inside `mapPartitions`, aggregate
locally. This is Listing 1 of the paper and the 68.10 s row of Table 2. It should
be published alongside the optimized variant so that both regimes reported in
Table 2 can be reproduced.
