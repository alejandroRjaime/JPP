// =============================================================================
// A `customer` dimension with the same row count but a sparse key space.
//
// The indexed-array lookup is sized by the LARGEST surrogate key, not by the
// number of rows. Every dimension measured so far has dense keys, so the two
// coincide and the structure came out at 0.97x the raw Parquet bytes. That says
// nothing about the case the paper itself flags as the precondition of the
// optimization, and which a reviewer asked about directly: whether the in-memory
// structure can end up larger than the data it replaces.
//
// With a spread factor f, keys are drawn from 0..(rows*f - 1) instead of
// 0..rows-1. The row count, the schema and the on-disk size stay essentially
// fixed while the array grows by f. The array is 4 bytes per slot whether or not
// a row occupies it, so the answer is arithmetic once the sizes are measured —
// but it is measured here rather than argued, and the query is timed on top,
// because an array that fits is not the same as an array that is worth building.
//
// The fact table is NOT regenerated. It references keys 0..2,999,999, and those
// keys must keep pointing at a row with the same `c_nation`, or the query would
// return a different answer and the comparison would be meaningless. So the
// first 3,000,000 rows keep their identity and their attributes; the sparsity is
// introduced by the remaining rows being scattered up to rows*f.
//
// Usage:  ./scripts/jpp GenerateSparseDims <outDir> <factors> <factLineorderDir>
//   e.g.  ./scripts/jpp GenerateSparseDims ~/ssb_sparse 10,100,1000 ~/ssb_synth/lineorder
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io.File
import java.nio.file.{Files, Paths}

object GenerateSparseDims {

  /** Keys the fact table references. Rows below this bound must keep both their
    * key and their attribute values. */
  final val FACT_CUSTKEYS = 3000000L

  /** Dimension partitioning, fixed rather than left to the environment.
    *
    * These dimension columns are deterministic functions of the row id, so the
    * partition count does not change their values the way it changes a `rand`
    * column. It does change the file count, and therefore the read parallelism
    * of every measurement taken over them, which is enough to make two runs
    * incomparable. The value is the one the already-published measurements were
    * taken at, so regenerating reproduces them rather than silently shifting
    * them. */
  final val DIM_PARTITIONS = 16


  private def dirBytes(path: String): Long = {
    val f = new File(path)
    if (!f.exists) 0L
    else if (f.isFile) f.length
    else Option(f.listFiles()).map(_.map(x => dirBytes(x.getPath)).sum).getOrElse(0L)
  }

  def main(args: Array[String]): Unit = {
    val outDir  = if (args.nonEmpty) args(0) else "/path/to/ssb_sparse"
    // Factors chosen to bracket the hard limit rather than to be round numbers.
    // The array is indexed by an Int, so it stops being addressable once the
    // largest key passes Int.MaxValue = 2,147,483,647. With 3,000,000 rows that
    // happens between f=700 (2.10e9, just inside) and f=1000 (3.00e9, outside),
    // so the boundary is measured rather than extrapolated.
    val factors = if (args.length > 1) args(1).split(",").map(_.trim.toLong).toSeq
                  else Seq(10L, 100L, 500L, 700L, 1000L)
    val factDir = if (args.length > 2) Some(args(2)) else None
    val cores   = if (args.length > 3) args(3) else "16"

    // Key distribution.
    //
    //   tip        the construction of §12: the 3,000,000 keys the fact table
    //              references stay dense, and one extra row sits at the top of
    //              the range. This gives the correct array size for the spread
    //              factor while occupying the least possible disk, so the
    //              lookup/disk ratios it produces are upper bounds.
    //
    //   scattered  the same row count, with the keys spread irregularly across
    //              the whole range: key = id*f + jitter, with jitter in [0, f),
    //              which is uniform over the range and unique by construction.
    //              This is what a real sparse key space looks like, and it costs
    //              more on disk because irregular keys do not delta-encode the
    //              way a dense run does. Measuring how much more is the point.
    //
    // The trade-off is unavoidable and is stated rather than hidden: `scattered`
    // moves the keys the fact table references, so most fact rows no longer find
    // a dimension row and the queries do not return the published answer. Sizes
    // remain fully comparable — DimStats does not run a query — but timings do
    // not, and none are reported for this mode. Keeping the answer would require
    // holding 0..2,999,999 dense, which is precisely what leaves no room to
    // scatter at a fixed row count.
    val distribution = if (args.length > 4) args(4) else "tip"
    require(Set("tip", "scattered").contains(distribution),
      s"unknown distribution: $distribution (expected tip or scattered)")

    val spark = SparkSession.builder()
      .appName("GenerateSparseDims")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val regions = Array("AMERICA", "ASIA", "EUROPE", "AFRICA", "MIDDLE EAST")
    val mfgrs   = Array("MFGR#1", "MFGR#2", "MFGR#3", "MFGR#4", "MFGR#5")
    val nations = Array(
      "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "CHINA",
      "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA",
      "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN",
      "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "ROMANIA",
      "RUSSIA", "SAUDI ARABIA", "UNITED KINGDOM", "UNITED STATES", "VIETNAM")

    factors.foreach { f =>
      val suffix = if (distribution == "scattered") "s" else ""
      val dir    = s"$outDir/sparse${f}x$suffix"
      val maxKey = FACT_CUSTKEYS * f

      // Rows 0..2,999,999 keep both their key and their attributes, so every
      // fact row still resolves to the same nation and the query returns the
      // same answer. Sparsity is introduced without touching them.
      val dense = spark.range(0, FACT_CUSTKEYS, 1, DIM_PARTITIONS)
        .withColumn("c_custkey", $"id")
        .withColumn("c_nation", element_at(lit(nations), (($"id" % 25) + 1).cast("int")))
        .withColumn("c_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
        .select("c_custkey", "c_nation", "c_region")

      // One row at the top of the key space is enough, and is the right
      // instrument: the array is sized by the largest key alone, so a single
      // row at rows*f-1 produces exactly the same array as a dimension whose
      // keys are scattered uniformly over that range.
      //
      // What it does change is the denominator. Scattering more rows would
      // enlarge the Parquet file — and scattered keys compress worse than dense
      // ones, enlarging it further — which would *lower* the lookup/disk ratio.
      // So the ratio measured here is an upper bound on that ratio, which is
      // the conservative direction for the question being asked. It is also the
      // only construction that holds row count, schema and attribute values
      // fixed while varying nothing but the key range.
      //
      // The rows the fact table probes are untouched, so every query still
      // returns the same answer at every spread factor.
      val tip = spark.range(0, 1)
        .withColumn("c_custkey", lit(maxKey - 1))
        .withColumn("c_nation", lit(nations(0)))
        .withColumn("c_region", lit(regions(0)))
        .select("c_custkey", "c_nation", "c_region")

      // Same row count as `tip`, keys spread irregularly over the whole range.
      // `id * f` alone would be perfectly regular and would still delta-encode
      // almost as well as a dense run, which would understate the cost; the
      // jitter is what makes the distribution realistic. Uniqueness holds
      // because consecutive base values differ by f and the jitter is below f.
      val scattered = spark.range(0, FACT_CUSTKEYS, 1, DIM_PARTITIONS)
        .withColumn("c_custkey", ($"id" * f + (rand(7) * f).cast("long")))
        .withColumn("c_nation", element_at(lit(nations), (($"id" % 25) + 1).cast("int")))
        .withColumn("c_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
        .select("c_custkey", "c_nation", "c_region")

      val customer = if (distribution == "scattered") scattered else dense.union(tip)
      customer.write.mode("overwrite").parquet(s"$dir/customer")

      // Other dimensions unchanged: one variable at a time.
      spark.range(0, 200000L, 1, DIM_PARTITIONS)
        .withColumn("s_suppkey", $"id")
        .withColumn("s_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
        .select("s_suppkey", "s_region")
        .write.mode("overwrite").parquet(s"$dir/supplier")

      spark.range(0, 1400000L, 1, DIM_PARTITIONS)
        .withColumn("p_partkey", $"id")
        .withColumn("p_mfgr", element_at(lit(mfgrs), (($"id" % 5) + 1).cast("int")))
        .select("p_partkey", "p_mfgr")
        .write.mode("overwrite").parquet(s"$dir/part")

      spark.range(0, 2556L, 1, DIM_PARTITIONS)
        .withColumn("d_datekey", $"id")
        .withColumn("d_year", (lit(1992) + ($"id" / 365).cast("int")).cast("string"))
        .select("d_datekey", "d_year")
        .write.mode("overwrite").parquet(s"$dir/date")

      factDir.foreach { src =>
        val link = Paths.get(s"$dir/lineorder")
        if (!Files.exists(link)) Files.createSymbolicLink(link, Paths.get(src).toAbsolutePath)
      }

      val bytes   = dirBytes(s"$dir/customer")
      val nRows   = if (distribution == "scattered") FACT_CUSTKEYS else FACT_CUSTKEYS + 1
      println(f"spread ${f}%5dx [$distribution%9s] -> rows=$nRows%,12d  " +
              f"maxKey~=${maxKey - 1}%,15d  disk=$bytes%,12d  " +
              f"predicted array=${4L * maxKey}%,15d bytes")
    }

    println(s"\nwritten to $outDir")
    spark.stop()
  }
}
