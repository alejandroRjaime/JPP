// =============================================================================
// Rewrites the fact table partitioned by year, for the physical-design
// experiment: a query aligned with the partitioning key prunes, and the
// question is whether pruning changes the ratio between the strategies or only
// their absolute times.
//
// The year is derived from the date surrogate key and written as a Hive-style
// partition column, so Spark can prune whole directories when the query
// restricts it. The original unpartitioned fact table is left untouched.
//
// Usage:  spark-submit --class PartitionFact <jar> <dataDir> [outSubdir]
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object PartitionFact {

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val outName = if (args.length > 1) args(1) else "lineorder_by_year"
    val cores   = if (args.length > 2) args(2) else "16"

    val spark = SparkSession.builder()
      .appName("PartitionFact")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val lo = spark.read.parquet(s"$dataDir/lineorder")

    // Same derivation the date dimension uses, so lo_year and d_year agree.
    val withYear = lo.withColumn("lo_year", lit(1992) + ($"lo_orderdate" / 365).cast("int"))

    withYear.write
      .mode("overwrite")
      .partitionBy("lo_year")
      .parquet(s"$dataDir/$outName")

    val out = spark.read.parquet(s"$dataDir/$outName")
    println(s"written to $dataDir/$outName")
    println(s"rows       = ${out.count()}")
    println(s"partitions on disk:")
    out.groupBy("lo_year").count().orderBy("lo_year").show(20, false)

    spark.stop()
  }
}
