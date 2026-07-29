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
    spark.range(0, nCust)
      .withColumn("c_custkey", $"id")
      .withColumn("c_nation", ($"id" % nNation).cast("string"))
      .select("c_custkey", "c_nation")
      .write.mode("overwrite").parquet(s"$outDir/customer")

    spark.range(0, nSupp)
      .withColumn("s_suppkey", $"id")
      .select("s_suppkey")
      .write.mode("overwrite").parquet(s"$outDir/supplier")

    spark.range(0, nPart)
      .withColumn("p_partkey", $"id")
      .select("p_partkey")
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
