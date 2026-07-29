// =============================================================================
// Spark baselines: Sort-Merge Join and forced Broadcast Hash Join.
//
// These are the intra-Spark comparison of the paper: same runtime, same
// hardware, same storage, same data. Only the physical execution changes.
//
// VERIFY BEFORE PUBLISHING: the projection must match the one the Joinless
// implementation performs. If these baselines carry more dimension columns
// through the join than the pattern materializes, the comparison is not
// equivalent and the reported times are not comparable.
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkBaselines {

  private def starQuery(spark: SparkSession, dataDir: String, broadcastDims: Boolean): DataFrame = {
    val cust = spark.read.parquet(s"$dataDir/customer").select("c_custkey", "c_nation")
    val supp = spark.read.parquet(s"$dataDir/supplier").select("s_suppkey")
    val part = spark.read.parquet(s"$dataDir/part").select("p_partkey")
    val dat  = spark.read.parquet(s"$dataDir/date").select("d_datekey", "d_year")
    val lo   = spark.read.parquet(s"$dataDir/lineorder")

    def d(df: DataFrame) = if (broadcastDims) broadcast(df) else df

    lo.join(d(cust), lo("lo_custkey")   === cust("c_custkey"))
      .join(d(supp), lo("lo_suppkey")   === supp("s_suppkey"))
      .join(d(part), lo("lo_partkey")   === part("p_partkey"))
      .join(d(dat),  lo("lo_orderdate") === dat("d_datekey"))
      .groupBy("d_year", "c_nation")
      .agg(sum("amount").alias("total"))
  }

  /** Sort-Merge Join: implicit broadcasting disabled, so Spark shuffles the fact
    * table once per dimension. */
  def sortMergeJoin(spark: SparkSession, dataDir: String): DataFrame = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    starQuery(spark, dataDir, broadcastDims = false)
  }

  /** Broadcast Hash Join: every dimension force-broadcast via hint. */
  def broadcastHashJoin(spark: SparkSession, dataDir: String): DataFrame = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    starQuery(spark, dataDir, broadcastDims = true)
  }

  def main(args: Array[String]): Unit = {
    val dataDir  = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val strategy = if (args.length > 1) args(1) else "smj"

    val spark = SparkSession.builder()
      .appName(s"SparkBaseline-$strategy")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[16]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    val t0 = System.nanoTime()
    val result = (strategy match {
      case "bhj" => broadcastHashJoin(spark, dataDir)
      case _     => sortMergeJoin(spark, dataDir)
    }).cache()

    val groups  = result.count()
    val total   = result.agg(sum("total")).head().getDouble(0)
    val elapsed = (System.nanoTime() - t0) / 1e9

    println(f"strategy      = $strategy")
    println(f"groups        = $groups")
    println(f"total (check) = $total%.1f")
    println(f"elapsed       = $elapsed%.2f s")

    spark.stop()
  }
}
