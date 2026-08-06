# The generator: reconstruction of the published dataset

Measured 2026-08-05. Conclusion first: **the dataset is reconstructible, and the
reconstruction is verified.** `datagen/GenerateSSBv2.scala` reproduces
`~/ssb_synth/lineorder` exactly.

This reverses the working assumption of `reports/experimentos_revision.md` §8, which
recorded that the dataset could not be regenerated from the repository. That was correct
about `datagen/GenerateSSB.scala` and wrong about reconstructibility in general.

---

## 1. The real schema

### Fact table — `~/ssb_synth/lineorder`

23.4 GB in **2000 Parquet files**, each holding exactly 300,000 contiguous rows.

| Column | Type | Min | Max | Distinct |
|---|---|---:|---:|---:|
| `lo_orderkey` | `bigint` | 0 | 599,999,999 | 600,000,000 |
| `lo_custkey` | `bigint` | 0 | 2,999,999 | — |
| `lo_suppkey` | `bigint` | 0 | 199,999 | — |
| `lo_partkey` | `bigint` | 0 | 1,399,999 | — |
| `lo_orderdate` | `bigint` | 0 | 2,555 | — |
| `amount` | `double` | 1.0103e-05 | 9999.999996 | — |

600,000,000 rows. No nulls. `lo_orderkey` is distinct in every row and equals the row
ordinal.

`amount`: mean 5000.130159, sum 3,000,078,095,504.6, and **zero integer values** in
600 million rows. It is a continuous uniform draw on (0, 10000), not the
`(id % 10000) + 1` of `GenerateSSB.scala`. The sum sits 78,095,504 above the 3×10¹²
expectation, which is 1.10σ for 6×10⁸ uniform samples — consistent with a random draw and
not with any modular rule.

### Dimensions

| Table | Rows | Columns | On disk (B) |
|---|---:|---|---:|
| `customer` | 3,000,000 | `c_custkey` bigint, `c_nation` varchar, `c_region` varchar | 12,348,481 |
| `supplier` | 200,000 | `s_suppkey` bigint, `s_region` varchar | 825,037 |
| `part` | 1,400,000 | `p_partkey` bigint, `p_mfgr` varchar | 5,697,648 |
| `date` | 2,556 | `d_datekey` bigint, `d_year` varchar | 23,148 |

Keys are dense in 0…N−1. `c_nation` holds the 25 TPC-H nation names, `c_region` and
`s_region` five region names, `p_mfgr` five manufacturer codes, `d_year` the eight values
1992–1999 (the last covering a single day, since 2555 = 365 × 7 exactly).

**The dimensions were already reproducible.** `GenerateSSB.scala` generates them as
deterministic functions of the row id, and regenerating `customer` produces a file of
12,348,481 bytes — identical to the published one. Only the fact table was in question.

---

## 2. How the rule was recovered

The keys and `amount` are random draws, so the table is reproducible only if the RNG seed
can be recovered. Three properties of the published data made a bounded search possible:

1. The fact table is written as **2000 contiguous partitions**, not shuffled: file
   `part-00000` holds `lo_orderkey` 0…299,999. So row 0 is the first row of partition 0.
2. Spark seeds `rand(seed)` per partition as `XORShiftRandom(seed + partitionIndex)`.
   For partition 0 the first draw is therefore a function of the seed alone.
3. Row 0's stored values are exactly those first draws.

Searching seeds for a first draw that reproduces row 0 is then a one-dimensional search.

### A replica that had to be discarded

The first attempt reimplemented `XORShiftRandom` from its published algorithm. It was
validated against Spark before use, and **it did not match**: for seed 12345, partition 0,
Spark produces 0.35343661019324624 and the replica produced 0.71926634542343780. Four other
constructions (unhashed seed, `java.util.Random`, hashed `java.util.Random`, per-partition
hash) were tried and none matched either.

A search built on that replica would have returned no match and supported a confident,
false conclusion that the dataset was unreconstructible.

The fix was to stop replicating: `org.apache.spark.util.random.XORShiftRandom` is
`private[spark]` in Scala, which is a compile-time restriction only — the bytecode is
public. Reached by reflection, it reproduces Spark's value exactly, and the search rests on
the implementation actually in use.

```bash
./scripts/jpp SeedSearch sparkgen  12345 2 3      # ground truth from Spark
./scripts/jpp SeedSearch calibrate 0.35343661019324624 12345 0
```

### The search

```bash
./scripts/jpp SeedSearch search 239.06964275028918 0 20000000 0 10000
#   MATCH Spark rand seed=5  ->  239.06964275028918
#   searched 20,000,000 seeds in 1.2 s; 1 match
```

Row 0's `amount` is 239.06964275028918. The comparison scales the draw up by 10000 rather
than dividing the stored value down, so no rounding step is introduced that the original
computation did not perform. Exactly one seed in twenty million reproduces it.

The four key columns were then checked against seeds 0–11 with a tolerance of one unit,
since those columns are floored to integers:

| Column | Row 0 value | Seed | `rand(seed) × range` |
|---|---:|---:|---:|
| `lo_custkey` | 1,909,136 | **1** | 1,909,136.2846 |
| `lo_suppkey` | 106,224 | **2** | 106,224.1445 |
| `lo_partkey` | 360,334 | **3** | 360,334.0091 |
| `lo_orderdate` | 2,436 | **4** | 2,436.2395 |
| `amount` | 239.0696… | **5** | 239.0696… |

Consecutive seeds 1–5, one per column, in schema order.

---

## 3. The recovered rule

```scala
spark.range(0, 600000000L, 1, 2000)
  .withColumn("lo_orderkey",  $"id")
  .withColumn("lo_custkey",   (rand(1) * 3000000).cast("long"))
  .withColumn("lo_suppkey",   (rand(2) *  200000).cast("long"))
  .withColumn("lo_partkey",   (rand(3) * 1400000).cast("long"))
  .withColumn("lo_orderdate", (rand(4) *    2556).cast("long"))
  .withColumn("amount",        rand(5) * 10000.0)
```

The partitioning is part of the rule, not a performance detail. `rand` is seeded per
partition, so generating the same range with a different partition count — or inserting a
`repartition`, as `GenerateSSB.scala` does — changes **every value in the table**.

---

## 4. Verification

```bash
./scripts/jpp VerifyGenerator ~/ssb_synth
```

Per-column checksums over all 600,000,000 rows of both tables. The regenerated table is
never written to disk; it is consumed directly from the same expressions
`GenerateSSBv2.scala` writes.

| Checksum | Published | Regenerated | |
|---|---:|---:|---|
| `n` | 600,000,000 | 600,000,000 | MATCH |
| `sum_orderkey` | 179,999,999,700,000,000 | 179,999,999,700,000,000 | MATCH |
| `sum_custkey` | 900,022,254,009,281 | 900,022,254,009,281 | MATCH |
| `sum_suppkey` | 60,001,169,805,653 | 60,001,169,805,653 | MATCH |
| `sum_partkey` | 420,010,238,860,186 | 420,010,238,860,186 | MATCH |
| `sum_orderdate` | 766,518,981,463 | 766,518,981,463 | MATCH |
| **`sum_amount_bits`** | 2,796,171,100,739,202,574,311,426,188 | 2,796,171,100,739,202,574,311,426,188 | **MATCH** |

`sum_amount_bits` is the sum of the IEEE-754 bit patterns of `amount`. It is exact and
independent of summation order, and it agrees only if every one of the 600 million values
is identical bit for bit.

One quantity differs and is deliberately excluded from the verdict:

| Diagnostic | Published | Regenerated |
|---|---:|---:|
| `sum_amount_double` | 3,000,078,095,504.6140 | 3,000,078,095,504.6160 |

Summing a `double` column in parallel is order-dependent, and the two tables are read
through different physical layouts — Parquet splits on one side, range partitions on the
other — so the reduction trees differ even when every value is identical. The first version
of the verification tool let this decide, and reported a mismatch for a table that matches
bit for bit; the order-independent bit-sum exists precisely so that it does not have to.

**Result: the generator reproduces the published fact table.**

---

## 5. What this means for the paper

- The repository can now regenerate the dataset every table rests on. A reviewer asking
  for reproducibility can be given `datagen/GenerateSSBv2.scala` and
  `datagen/VerifyGenerator.scala` and check the result themselves.
- `datagen/GenerateSSB.scala` should be **removed or clearly marked superseded**. It
  produces a different dataset — different schema, different values, an aggregate sum of
  3,000,300,000,000 against the real 3,000,078,095,504.6 — while sitting in the repository
  as if it were the generator of record. Its own header warns about this, but a header is
  not enough when the file name says otherwise.
- The 2000-partition layout should be stated wherever the dataset is described. It
  determines both the values and the scan parallelism, and it is the reason the paper's
  fact table reports 250 Spark partitions on read.

### If it had not been reconstructible

Recorded because it was the expected outcome and remains the fallback for any future
dataset whose seed is not recoverable. The minimum a third party needs to confirm they hold
the same data, without being able to regenerate it:

- a schema manifest — column names, types, nullability, per-column min/max;
- exact row counts for the fact table and every dimension;
- per-column checksums of the kind used in §4, including an order-independent one for any
  floating-point column;
- the group count and aggregate sum of every benchmark query;
- the physical layout: file count, rows per file, and whether partitions are contiguous;
- per-file hashes, if byte-level identity is required.

`VerifyGenerator.scala` already emits most of this and would serve as the manifest tool.
