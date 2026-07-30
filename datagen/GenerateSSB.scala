// =============================================================================
// Synthetic star-schema data generator.
//
// RECONSTRUCTED — NOT THE GENERATOR USED FOR THE PAPER.
// Replace this file with the generator that actually produced ~/ssb_synth
// before publishing, or re-run the benchmark against this one and update the
// reported numbers. Publishing a generator that does not reproduce the reported
// verification sum is worse than publishing none.
//
// The cardinalities below match those stated in the paper. Surrogate keys are
// dense (0..N), which is the precondition of the indexed-array lookup.
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object GenerateSSB {

  def main(args: Array[String]): Unit = {
    val outDir  = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val factRows = if (args.length > 1) args(1).toLong else 600000000L
    val files    = if (args.length > 2) args(2).toInt  else 96

    val spark = SparkSession.builder()
      .appName("GenerateSSB")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[16]"))
      .getOrCreate()

    import spark.implicits._

    val nCust = 3000000L
    val nSupp = 200000L
    val nPart = 1400000L
    val nDate = 2556L      // 7 years of days
    val nNation = 25

    // --- dimensions: dense surrogate keys ---
    // Regions and manufacturers carry the predicates of Q2 and Q3. They live only on
    // the dimensions, so the query set can be extended without regenerating the 24 GB
    // fact table: rerun with --dims-only to rewrite the dimensions alone.
    val regions = Array("AMERICA", "ASIA", "EUROPE", "AFRICA", "MIDDLE EAST")
    val mfgrs   = Array("MFGR#1", "MFGR#2", "MFGR#3", "MFGR#4", "MFGR#5")

    spark.range(0, nCust)
      .withColumn("c_custkey", $"id")
      .withColumn("c_nation", ($"id" % nNation).cast("string"))
      .withColumn("c_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
      .select("c_custkey", "c_nation", "c_region")
      .write.mode("overwrite").parquet(s"$outDir/customer")

    spark.range(0, nSupp)
      .withColumn("s_suppkey", $"id")
      .withColumn("s_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
      .select("s_suppkey", "s_region")
      .write.mode("overwrite").parquet(s"$outDir/supplier")

    spark.range(0, nPart)
      .withColumn("p_partkey", $"id")
      .withColumn("p_mfgr", element_at(lit(mfgrs), (($"id" % 5) + 1).cast("int")))
      .select("p_partkey", "p_mfgr")
      .write.mode("overwrite").parquet(s"$outDir/part")

    spark.range(0, nDate)
      .withColumn("d_datekey", $"id")
      .withColumn("d_year", (lit(1992) + ($"id" / 365).cast("int")).cast("string"))
      .select("d_datekey", "d_year")
      .write.mode("overwrite").parquet(s"$outDir/date")

    // --- fact table: uniformly distributed foreign keys ---
    // Written as `files` files of roughly equal size so that the benchmark does
    // not need to repartition inside the measured region.
    spark.range(0, factRows)
      .repartition(files)
      .withColumn("lo_custkey",   ($"id" % nCust))
      .withColumn("lo_suppkey",   ($"id" % nSupp))
      .withColumn("lo_partkey",   ($"id" % nPart))
      .withColumn("lo_orderdate", ($"id" % nDate))
      .withColumn("amount",       (($"id" % 10000) + 1).cast("double"))
      .select("lo_custkey", "lo_suppkey", "lo_partkey", "lo_orderdate", "amount")
      .write.mode("overwrite").parquet(s"$outDir/lineorder")

    println(s"written to $outDir")
    spark.stop()
  }
}
