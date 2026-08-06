// =============================================================================
// Star-schema generator that reproduces ~/ssb_synth exactly.
//
// datagen/GenerateSSB.scala does NOT produce the dataset the paper's numbers
// were taken over: it derives every foreign key from the row id by modular
// arithmetic and makes `amount` an integer, whereas the published dataset draws
// all five from a random generator. Two checks establish that directly:
// the real fact table has a `lo_orderkey` column GenerateSSB never writes, and
// its aggregate sum is 3,000,078,095,504.6 where GenerateSSB's formula implies
// exactly 3,000,300,000,000.
//
// The generating rule was recovered rather than guessed. The fact table is
// written as 2000 contiguous partitions of 300,000 rows, so row 0 is the first
// row of partition 0 and its stored values are the first draws of each column's
// generator. Spark seeds `rand(seed)` per partition as
// `XORShiftRandom(seed + partitionIndex)`, so for partition 0 the first draw is
// a function of the seed alone, and the seed can be searched for. See
// datagen/SeedSearch.scala; the search is reproduced by
// scripts/verify_generator.sh.
//
// Recovered rule — one seed per column, consecutive from 1:
//
//   lo_orderkey  = id
//   lo_custkey   = (rand(1) * 3000000).cast(long)
//   lo_suppkey   = (rand(2) *  200000).cast(long)
//   lo_partkey   = (rand(3) * 1400000).cast(long)
//   lo_orderdate = (rand(4) *    2556).cast(long)
//   amount       =  rand(5) * 10000.0
//
// The dimensions are unchanged from GenerateSSB: they are deterministic
// functions of the row id and already reproduce byte for byte (the regenerated
// `customer` is 12,348,481 bytes, identical to the published one).
//
// Usage:  ./scripts/jpp GenerateSSBv2 <outDir> [factRows] [partitions] [--dims-only]
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object GenerateSSBv2 {

  // Cardinalities of the published dataset, confirmed against it: every key
  // column covers its full range and no other.
  final val N_CUST = 3000000L
  final val N_SUPP =  200000L
  final val N_PART = 1400000L
  final val N_DATE =    2556L
  final val N_NATION  = 25
  final val AMOUNT_MAX = 10000.0

  // The fact table's physical layout is part of the measurement: 2000 files of
  // 300,000 rows. Changing it changes the scan parallelism and therefore every
  // time in the paper, so it is fixed here rather than left to a default.
  final val FACT_ROWS       = 600000000L
  final val FACT_PARTITIONS = 2000

  def main(args: Array[String]): Unit = {
    val outDir   = if (args.nonEmpty) args(0) else "/path/to/ssb_synth"
    val dimsOnly = args.contains("--dims-only")
    val rest     = args.drop(1).filterNot(_.startsWith("--"))
    val factRows = if (rest.length > 0) rest(0).toLong else FACT_ROWS
    val parts    = if (rest.length > 1) rest(1).toInt  else FACT_PARTITIONS

    val spark = SparkSession.builder()
      .appName("GenerateSSBv2")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[16]"))
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

    // --- dimensions: deterministic in the row id, dense surrogate keys -------
    spark.range(0, N_CUST)
      .withColumn("c_custkey", $"id")
      .withColumn("c_nation", element_at(lit(nations), (($"id" % N_NATION) + 1).cast("int")))
      .withColumn("c_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
      .select("c_custkey", "c_nation", "c_region")
      .write.mode("overwrite").parquet(s"$outDir/customer")

    spark.range(0, N_SUPP)
      .withColumn("s_suppkey", $"id")
      .withColumn("s_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
      .select("s_suppkey", "s_region")
      .write.mode("overwrite").parquet(s"$outDir/supplier")

    spark.range(0, N_PART)
      .withColumn("p_partkey", $"id")
      .withColumn("p_mfgr", element_at(lit(mfgrs), (($"id" % 5) + 1).cast("int")))
      .select("p_partkey", "p_mfgr")
      .write.mode("overwrite").parquet(s"$outDir/part")

    spark.range(0, N_DATE)
      .withColumn("d_datekey", $"id")
      .withColumn("d_year", (lit(1992) + ($"id" / 365).cast("int")).cast("string"))
      .select("d_datekey", "d_year")
      .write.mode("overwrite").parquet(s"$outDir/date")

    // --- fact table ---------------------------------------------------------
    if (!dimsOnly) {
      // `range(start, end, step, numPartitions)` gives contiguous partitions and
      // no exchange. This matters twice over: it is the layout the measurements
      // were taken against, and `rand` is seeded per partition, so a shuffle
      // here would change every value in the table.
      spark.range(0, factRows, 1, parts)
        .withColumn("lo_orderkey",  $"id")
        .withColumn("lo_custkey",   (rand(1) * N_CUST).cast("long"))
        .withColumn("lo_suppkey",   (rand(2) * N_SUPP).cast("long"))
        .withColumn("lo_partkey",   (rand(3) * N_PART).cast("long"))
        .withColumn("lo_orderdate", (rand(4) * N_DATE).cast("long"))
        .withColumn("amount",        rand(5) * AMOUNT_MAX)
        .select("lo_orderkey", "lo_custkey", "lo_suppkey", "lo_partkey", "lo_orderdate", "amount")
        .write.mode("overwrite").parquet(s"$outDir/lineorder")
    }

    println(if (dimsOnly) s"dimensions rewritten in $outDir (fact table untouched)"
            else s"written to $outDir  ($factRows rows, $parts partitions)")
    spark.stop()
  }
}
