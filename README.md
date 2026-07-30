# The Joinless Partition Pattern

Reference implementation and benchmark code for *The Joinless Partition Pattern: A
Spark-Native Physical Execution Pattern for Multi-Dimensional Aggregations in Apache
Spark* (under review, *Distributed and Parallel Databases*, Springer).

The pattern replaces a chain of physical join operators with a single `mapPartitions`
pass that performs broadcast-resident dimension lookups, partition-local enrichment and
pre-aggregation, so that only compact partial aggregates are shuffled.

---

## Contents

| Path | What it is |
|---|---|
| `src/main/scala/JoinlessPartitionPattern.scala` | Optimized implementation of the pattern |
| `src/main/scala/JoinlessQueries.scala` | The pattern over the four-query benchmark set |
| `baselines/SparkBaselines.scala` | Sort-Merge and Broadcast Hash Join baselines (Q1) |
| `baselines/SparkBaselineQueries.scala` | The same baselines over the full query set |
| `baselines/JoinlessBase.scala` | *Not yet added* — base `HashMap` variant, the 68.10 s configuration |
| `sql/queries.sql` | The four benchmark queries |
| `sql/clickhouse.sql` | ClickHouse schema and load |
| `sql/duckdb.sql` | DuckDB schema and load |
| `datagen/GenerateSSB.scala` | Star-schema data generator |
| `scripts/run_queryset.sh` | Full benchmark: 4 queries × 3 Spark configurations × 5 runs |
| `scripts/run_all.sh` | Q1 only, all configurations |

---

## Requirements

- JDK 17
- Apache Spark 3.5.8 (Scala 2.12, Hadoop 3.3.4)
- DuckDB 1.5.3
- ClickHouse 26.6
- ~60 GB free disk for the dataset at the reported scale

## Hardware used for the reported results

Single workstation, 32-core CPU, 128 GB RAM, NVMe local storage, 1 Gbit Ethernet link to
a MinIO object store. Spark in local mode with `local[16]` and a 32 GB driver heap,
adaptive query execution disabled. Every engine constrained to 16 worker threads and
32 GB of memory.

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

Q4 is the direct test of the structural claim: the join-based baselines perform one
redistribution of the fact table per dimension, so they should improve markedly from
*N*=4 to *N*=2, while the pattern — whose shuffle count is independent of *N* — should
improve only to the extent that fewer lookups run per row.

Full SQL in [`sql/queries.sql`](sql/queries.sql).

---

## Reproducing the results

### 1. Build

```bash
sbt package
```

### 2. Generate the dataset

```bash
spark-submit --class GenerateSSB --master 'local[16]' \
  target/scala-2.12/jpp_2.12-1.0.jar ~/ssb_synth
```

Produces a `lineorder` fact table of 6×10⁸ rows (~24 GB uncompressed Parquet) and four
dimensions: `customer` (3M), `supplier` (200K), `part` (1.4M), `date` (2.5K). Surrogate
keys are dense, which is the precondition of the indexed-array lookup.

The predicate attributes used by Q2 and Q3 — `c_region`, `s_region`, `p_mfgr` — live only
on the dimensions, so the query set can be extended without regenerating the fact table.

### 3. Run the Spark configurations

```bash
./scripts/run_queryset.sh ~/ssb_synth     # all four queries
./scripts/run_all.sh ~/ssb_synth          # Q1 only
```

### 4. Run the external engines

```bash
duckdb < sql/duckdb.sql
clickhouse-client --queries-file sql/clickhouse.sql
```

### 5. Verify functional equivalence

For Q1, every configuration must produce **175 groups** and an aggregate sum of

```
3,000,078,095,504.6
```

For Q2–Q4, all five systems must agree with each other on the group count and the
aggregate sum. Check this before comparing any timing — a configuration that computed
something different is not a baseline.

---

## Reported results — Q1

Median of five runs, first discarded as warm-up.

### Locally-attached storage

| System | Time (s) | vs. Joinless |
|---|---:|---:|
| DuckDB | 7.14 | 0.76× |
| **Joinless Partition Pattern** | **9.41** | **1.00×** |
| ClickHouse | 13.73 | 1.46× |
| Spark Broadcast Hash Join | 39.77 | 4.23× |
| Spark Sort-Merge Join | 126.87 | 13.48× |

### Implementation optimization path

| Implementation | Time (s) | Cumulative speedup |
|---|---:|---:|
| `HashMap` + `getOrElse` (base) | 68.10 | 1.00× |
| Indexed array, String values | 20.80 | 3.27× |
| Indexed array (Int) + packed-`Long` key | 9.41 | 7.24× |

### Disaggregated object storage

| System | Local (s) | MinIO (s) | Penalty |
|---|---:|---:|---:|
| Joinless Partition Pattern | 9.41 | 9.66 | 1.03× |
| DuckDB | 7.14 | 7.44 | 1.04× |
| Spark Sort-Merge Join | 126.87 | 231.01 | 1.82× |
| Spark Broadcast Hash Join | 39.77 | 148.39 | 3.73× |

Per-query results for Q2–Q4 are reported in the paper.

---

## Engine configuration

Tuning can dominate cross-engine comparisons, so it is recorded rather than left implicit.

**Spark** runs with adaptive query execution disabled, for a stable and reproducible
physical plan, and with implicit broadcasting disabled so that the Sort-Merge Join
baseline is not silently converted into a broadcast plan.

**ClickHouse** is not measured reading Parquet through its object-store table function,
which would compare network-bound reads against cached ones. Each table is loaded into a
local `MergeTree` table with an explicit `ORDER BY` on its key, and the query runs with
`max_threads = 16` to match the thread budget of the other engines. This is deliberately
favourable to ClickHouse: it is given a native columnar layout with a chosen sort key,
while the Spark configurations read Parquet.

**DuckDB** runs with `threads = 16` and a 32 GB memory limit.

No claim is made that these settings are optimal. The schemas and queries are published
here so that they can be inspected and improved.

---

## Preconditions of the optimized variant

The optimizations are implementation choices, not changes to the pattern. Each applies
under a stated condition:

- **Indexed-array lookup** requires **dense surrogate keys** in 0…N. With sparse or
  string keys the base `HashMap` variant applies (~3× slower, no precondition).
- **Packed-`Long` group key** requires low-cardinality group-by attributes that fit
  packed into a `Long`. The bound is checked at runtime.
- **Primitive-typed accumulation** with a raw `while` loop has no precondition.

The base pattern — broadcast, lookup, local pre-aggregation — carries no precondition.

---

## Note on fact-table partitioning

`Dataset.repartition` performs a full exchange of the fact table. The implementations here
do **not** repartition: the fact table is consumed as laid out on disk. If the Parquet
layout does not match the target parallelism, compact the files offline rather than
repartitioning inside the measured region.

---

## License

MIT — see `LICENSE`.

## Citation

See `CITATION.cff`.
