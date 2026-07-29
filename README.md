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
| `src/main/scala/JoinlessPartitionPattern.scala` | Optimized implementation — the configuration reported at 9.41 s |
| `baselines/JoinlessBase.scala` | Base variant with broadcast `HashMap` lookups — the 68.10 s configuration, and Listing 1 of the paper |
| `baselines/SparkBaselines.scala` | Spark Sort-Merge Join and forced Broadcast Hash Join baselines |
| `sql/clickhouse.sql` | ClickHouse schema and query |
| `sql/duckdb.sql` | DuckDB schema and query |
| `datagen/GenerateSSB.scala` | Synthetic star-schema data generator |
| `scripts/run_all.sh` | Driver script for the full benchmark |

---

## Requirements

- JDK 17
- Apache Spark 3.5.8 (Scala 2.12, Hadoop 3.3.4)
- DuckDB 1.5.3
- ClickHouse 26.6
- ~60 GB free disk for the generated dataset at the reported scale

## Hardware used for the reported results

Single workstation, 32-core CPU, 128 GB RAM, NVMe local storage, 1 Gbit Ethernet link to
a MinIO object store. Spark in local mode with `local[16]` and a 32 GB driver heap,
adaptive query execution disabled. Every engine constrained to 16 worker threads and
32 GB of memory.

---

## Reproducing the results

### 1. Generate the dataset

```bash
spark-submit --class GenerateSSB --master 'local[16]' \
  target/scala-2.12/jpp_2.12-1.0.jar ~/ssb_synth
```

Produces a `lineorder` fact table of 6×10⁸ rows (~24 GB uncompressed Parquet) and four
dimensions: `customer` (3M), `supplier` (200K), `part` (1.4M), `date` (2.5K).

### 2. Run the Spark configurations

```bash
./scripts/run_all.sh ~/ssb_synth
```

### 3. Run the external engines

```bash
duckdb < sql/duckdb.sql
clickhouse-client --queries-file sql/clickhouse.sql
```

### 4. Verify functional equivalence

Every configuration must produce **175 groups** and an aggregate sum of

```
3,000,078,095,504.6
```

A result that does not match this sum was not admitted into the paper. Check this before
comparing any timings — an engine that computed something different is not a baseline.

---

## Reported results

Median of five runs, first run discarded as warm-up.

### Locally-attached storage (Table 1)

| System | Time (s) | vs. Joinless |
|---|---:|---:|
| DuckDB | 7.14 | 0.76× |
| **Joinless Partition Pattern** | **9.41** | **1.00×** |
| ClickHouse | 13.73 | 1.46× |
| Spark Broadcast Hash Join | 39.77 | 4.23× |
| Spark Sort-Merge Join | 126.87 | 13.48× |

### Implementation optimization path (Table 2)

| Implementation | Time (s) | Cumulative speedup |
|---|---:|---:|
| `HashMap` + `getOrElse` (base) | 68.10 | 1.00× |
| Indexed array, String values | 20.80 | 3.27× |
| Indexed array (Int) + packed-`Long` key | 9.41 | 7.24× |

### Disaggregated object storage (Table 3)

| System | Local (s) | MinIO (s) | Penalty |
|---|---:|---:|---:|
| Joinless Partition Pattern | 9.41 | 9.66 | 1.03× |
| DuckDB | 7.14 | 7.44 | 1.04× |
| Spark Sort-Merge Join | 126.87 | 231.01 | 1.82× |
| Spark Broadcast Hash Join | 39.77 | 148.39 | 3.73× |

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

`Dataset.repartition` performs a full exchange of the fact table. The implementation here
does **not** repartition by default: the fact table is consumed as laid out on disk. If
the Parquet layout does not match the target parallelism, compact the files offline
rather than repartitioning inside the measured region.

---

## License

MIT — see `LICENSE`.

## Citation

See `CITATION.cff`.
