# The Joinless Partition Pattern

Reference implementation and benchmark code for *The Joinless Partition Pattern: A
Spark-Native Physical Execution Pattern for Multi-Dimensional Aggregations in Apache
Spark* (under review, *Distributed and Parallel Databases*, Springer).

The pattern replaces a chain of physical join operators with a single `mapPartitions`
pass that performs broadcast-resident dimension lookups, partition-local enrichment and
pre-aggregation, so that only compact partial aggregates are shuffled.

**The paper is the record of results.** This repository exists so that every figure in it
can be regenerated and every configuration behind it inspected. Timings are not reproduced
here, because absolute times depend on the load the host is carrying at the time; ratios
between configurations reproduce, absolute seconds do not. Re-running the optimization path
on a loaded host reproduced its ratio to within 1% and its absolute times 1.39× higher on
both rows.

---

## Contents

| Path | What it is |
|---|---|
| `src/main/scala/JoinlessPartitionPattern.scala` | Optimized implementation of the pattern |
| `src/main/scala/JoinlessQueries.scala` | The pattern over the four-query benchmark set |
| `baselines/SparkBaselines.scala` | Sort-Merge and Broadcast Hash Join baselines (Q1) |
| `baselines/SparkBaselineQueries.scala` | The same baselines over the full query set |
| `baselines/JoinlessBase.scala` | Base `HashMap` variant, before the optimizations |
| `sql/queries.sql` | The four benchmark queries |
| `sql/clickhouse.sql` | ClickHouse schema and load |
| `sql/duckdb.sql` | DuckDB schema and load |
| `datagen/GenerateSSBv2.scala` | Data generator of record — reproduces the published dataset |
| `datagen/VerifyGenerator.scala` | Checksum comparison of published against regenerated data |
| `scripts/jpp` | Launcher: pins JDK 17 and the temp, shuffle and native-library paths |
| `scripts/run_queryset.sh` | Full benchmark: four queries, all Spark configurations |
| `scripts/run_optpath.sh` | The implementation optimization path |
| `reports/` | Measurement reports, including what could not be measured and why |

Verify this table against your working tree before publishing: several scripts were added
while preparing the current revision and the list above may be incomplete.

---

## Requirements

- JDK 17 — Spark 3.5 does not support JDK 23, and `scripts/jpp` pins the version
- Apache Spark 3.5.8 (Scala 2.12, Hadoop 3.3.4)
- DuckDB 1.5.3
- ClickHouse 26.6
- ~60 GB free disk for the dataset at the reported scale

## Hardware used for the reported results

Single workstation, 32-core CPU, 128 GB RAM, NVMe local storage, 1 Gbit Ethernet link to
a MinIO object store. Spark in local mode with `local[16]` and a 32 GB driver heap,
adaptive query execution disabled. Every engine constrained to 16 worker threads and
32 GB of memory.

The host is not dedicated. Where a measurement was taken while unrelated services were
running, the reports in `reports/` say so.

---

## The query set

Four queries over an SSB-style star schema. Each varies one property of the workload
relative to Q1, so that the evaluation probes the conditions under which the pattern is
claimed to apply rather than only its most favourable point.

| Query | Dimensions | Predicates | What it exercises |
|---|---|---|---|
| Q1 | 4 | — | Base case: multi-dimension enrichment, high aggregation ratio |
| Q2 | 4 | 2 | Selective dimension predicates, folded into the lookup structures |
| Q3 | 4 | 1 | Reduced aggregation ratio — a larger result relative to the fact table |
| Q4 | 2 | — | Dependence on the number of dimensions *N* |

Full SQL in [`sql/queries.sql`](sql/queries.sql).

---

## Reproducing the results

### 1. Build

```bash
sbt package
```

### 2. If `/tmp` is mounted `noexec`

Snappy extracts its native library to a temp directory and maps it executable; on a
hardened system that mapping fails and every Parquet read or write dies with
`UnsatisfiedLinkError`. Check with `findmnt /tmp`. `scripts/jpp` points Snappy, the S3A
buffer and the shuffle directory elsewhere, which is why the scripts here go through it
rather than calling `spark-submit` directly.

### 3. Generate the dataset

```bash
./scripts/jpp GenerateSSBv2 ~/ssb_synth
```

Produces a `lineorder` fact table of 6×10⁸ rows (~24 GB of Parquet) and four dimensions:
`customer` (3M), `supplier` (200K), `part` (1.4M), `date` (2,556). Surrogate keys are dense
in 0…N−1.

Two properties of the layout are part of the definition of the dataset rather than of its
performance. The fact table is written as **2000 contiguous partitions** of 300,000 rows
each, and `rand` is seeded per partition, so generating the same range at any other
partition count produces a different table with the same schema and the same row count.
The read parallelism of 250 Spark partitions is a separate quantity from the write
partitioning.

`datagen/GenerateSSB.scala` has been superseded and does **not** produce this dataset. It
emits a different schema and a different rule for the measure column, giving an aggregate
sum of 3,000,300,000,000 against the real 3,000,078,095,504.6.

### 4. Verify that you have the same data

```bash
./scripts/jpp VerifyGenerator ~/ssb_synth
```

Compares seven checksums over all 6×10⁸ rows of the published and regenerated tables. The
decisive one is

```
sum_amount_bits = 2,796,171,100,739,202,574,311,426,188
```

the sum of the IEEE-754 bit patterns of the measure column. It is exact, independent of
summation order, and equal only if every value matches bit for bit. The ordinary
floating-point sum of that column is reported as a diagnostic and is *not* used for the
verdict: parallel summation is order-dependent, and two tables read through different
physical layouts produce different reduction trees even when every value is identical.

### 5. Run the Spark configurations

```bash
./scripts/run_queryset.sh ~/ssb_synth     # all four queries
./scripts/run_optpath.sh ~/ssb_synth      # the optimization path
```

Every run is gated by two checks that abort rather than warn. `PartitionGuard` emits the
partition count of the fact table and of each dimension and fails if any differs from the
value declared for the experiment. `DatasetFingerprint` verifies the dataset checksums
before each sweep. Both exist because a partition count that changes silently changes the
experiment.

### 6. Run the external engines

```bash
duckdb < sql/duckdb.sql
clickhouse-client --queries-file sql/clickhouse.sql
```

### 7. Verify functional equivalence

Every configuration must agree on the group count and the aggregate sum before any timing
is compared. A configuration that computed something different is not a baseline.

| Query | Groups | Aggregate sum |
|---|---:|---:|
| Q1 | 200 | 3,000,078,095,504.6 |
| Q2 | 40 | 120,015,240,211.4 |
| Q3 | 400 | 1,199,919,067,064.6 |
| Q4 | 200 | 3,000,078,095,504.6 |
| Q5 | 25 | 428,304,471,223.2 |

Relative tolerance 3×10⁻¹⁴.

---

## Reported results

In the paper. They are not duplicated here, so that there is one record of them rather
than two that can drift apart.

---

## Engine configuration

Tuning can dominate cross-engine comparisons, so it is recorded rather than left implicit.

**Spark** runs with adaptive query execution disabled, for a stable and reproducible
physical plan, and with `spark.sql.autoBroadcastJoinThreshold` set to `-1` so that the
Sort-Merge Join baseline is not silently converted into a broadcast plan; the Broadcast
Hash Join baseline is then forced by hint. Note that at the scale of the published dataset
the *default* threshold of 10 MiB already admits all four dimensions, because it is
compared against the optimizer's estimate of the projected relation — 7.51 MiB for the
largest — and not against the size on disk.

**ClickHouse** is not measured reading Parquet through its object-store table function,
which would compare network-bound reads against cached ones. Each table is loaded into a
local `MergeTree` table with an explicit `ORDER BY` on its key, and the query runs with
`max_threads = 16` to match the thread budget of the other engines. This is deliberately
favourable to ClickHouse.

**DuckDB** runs with `threads = 16` and a 32 GB memory limit.

No claim is made that these settings are optimal. The schemas and queries are published
here so that they can be inspected and improved.

---

## Preconditions of the optimized variant

The optimizations are implementation choices, not changes to the pattern. Each applies
under a stated condition:

- **Indexed-array lookup** requires **two independent conditions**, not one:
    - dense surrogate keys in 0…N. The array is sized by the largest key rather than by the
      row count, so at three million rows a key spread of ten already needs a structure 9.6×
      the size of the Parquet data, and a spread of seven hundred needs 548× — 8.4 GB
      resident for a dimension of 12 MB on disk;
    - the largest key must be addressable by an `Int`, that is below 2,147,483,647. At three
      million rows this binds at a spread of about 716; a dimension with perfectly dense keys
      violates it as soon as it exceeds 2.1×10⁹ rows.

  Where either fails, the base `HashMap` variant applies — 6.15× slower at the published
  scale, and with no precondition.
- **Packed-`Long` group key** requires low-cardinality group-by attributes that fit packed
  into a `Long`. The bound is checked at runtime.
- **Primitive-typed accumulation** with a raw `while` loop has no precondition.
- **Dimension key uniqueness** is required by the base pattern: the conversion to a map
  discards duplicate keys silently.

The base pattern — broadcast, lookup, local pre-aggregation — carries no precondition
beyond the last of these.

---

## Note on fact-table partitioning

`Dataset.repartition` performs a full exchange of the fact table. The implementations here
do **not** repartition: the fact table is consumed as laid out on disk. If the Parquet
layout does not match the target parallelism, compact the files offline rather than
repartitioning inside the measured region. `PartitionGuard` enforces this: a run whose
input partitioning differs from the declared value fails rather than reporting a number.

---

## License

MIT — see `LICENSE`.

## Citation

See `CITATION.cff`.