// =============================================================================
// Spark Sort-Merge Join and Broadcast Hash Join baselines over the four-query
// benchmark set. Same queries as sql/queries.sql, expressed as DataFrame plans.
//
// The projections are deliberately minimal — each dimension contributes only
// the columns the query needs — so that the baselines carry the same payload
// through the join that the Joinless implementation materializes. Widening
// them would make the comparison non-equivalent.
//
// Usage: spark-submit --class SparkBaselineQueries <jar> <dataDir> <q1..q4> <smj|bhj>
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkBaselineQueries {

  def run(spark: SparkSession, dataDir: String, query: String, strategy: String): DataFrame = {
    // Implicit broadcasting is disabled in both cases: SMJ must not be silently
    // converted into a broadcast plan, and BHJ is forced by explicit hint.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val bcast = strategy.toLowerCase == "bhj"
    def d(df: DataFrame): DataFrame = if (bcast) broadcast(df) else df

    val lo = spark.read.parquet(s"$dataDir/lineorder")

    query.toLowerCase match {
      case "q1" =>
        val c = spark.read.parquet(s"$dataDir/customer").select("c_custkey", "c_nation")
        val s = spark.read.parquet(s"$dataDir/supplier").select("s_suppkey")
        val p = spark.read.parquet(s"$dataDir/part").select("p_partkey")
        val t = spark.read.parquet(s"$dataDir/date").select("d_datekey", "d_year")
        lo.join(d(c), lo("lo_custkey")   === c("c_custkey"))
          .join(d(s), lo("lo_suppkey")   === s("s_suppkey"))
          .join(d(p), lo("lo_partkey")   === p("p_partkey"))
          .join(d(t), lo("lo_orderdate") === t("d_datekey"))
          .groupBy("d_year", "c_nation").agg(sum("amount").alias("total"))

      case "q2" =>
        val c = spark.read.parquet(s"$dataDir/customer")
                     .filter(col("c_region") === "AMERICA").select("c_custkey", "c_nation")
        val s = spark.read.parquet(s"$dataDir/supplier")
                     .filter(col("s_region") === "AMERICA").select("s_suppkey")
        val p = spark.read.parquet(s"$dataDir/part").select("p_partkey")
        val t = spark.read.parquet(s"$dataDir/date").select("d_datekey", "d_year")
        lo.join(d(c), lo("lo_custkey")   === c("c_custkey"))
          .join(d(s), lo("lo_suppkey")   === s("s_suppkey"))
          .join(d(p), lo("lo_partkey")   === p("p_partkey"))
          .join(d(t), lo("lo_orderdate") === t("d_datekey"))
          .groupBy("d_year", "c_nation").agg(sum("amount").alias("total"))

      case "q3" =>
        val c = spark.read.parquet(s"$dataDir/customer").select("c_custkey", "c_nation")
        val s = spark.read.parquet(s"$dataDir/supplier").select("s_suppkey")
        val p = spark.read.parquet(s"$dataDir/part")
                     .filter(col("p_mfgr").isin("MFGR#1", "MFGR#2")).select("p_partkey", "p_mfgr")
        val t = spark.read.parquet(s"$dataDir/date").select("d_datekey", "d_year")
        lo.join(d(c), lo("lo_custkey")   === c("c_custkey"))
          .join(d(s), lo("lo_suppkey")   === s("s_suppkey"))
          .join(d(p), lo("lo_partkey")   === p("p_partkey"))
          .join(d(t), lo("lo_orderdate") === t("d_datekey"))
          .groupBy("d_year", "c_nation", "p_mfgr").agg(sum("amount").alias("total"))

      case "q4" =>
        val c = spark.read.parquet(s"$dataDir/customer").select("c_custkey", "c_nation")
        val t = spark.read.parquet(s"$dataDir/date").select("d_datekey", "d_year")
        lo.join(d(c), lo("lo_custkey")   === c("c_custkey"))
          .join(d(t), lo("lo_orderdate") === t("d_datekey"))
          .groupBy("d_year", "c_nation").agg(sum("amount").alias("total"))

      case other =>
        throw new IllegalArgumentException(s"unknown query: $other (expected q1..q4)")
    }
  }

  def main(args: Array[String]): Unit = {
    val dataDir  = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val query    = if (args.length > 1) args(1) else "q1"
    val strategy = if (args.length > 2) args(2) else "smj"
    val cores    = if (args.length > 3) args(3) else "16"

    val spark = SparkSession.builder()
      .appName(s"SparkBaseline-$query-$strategy")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    val t0      = System.nanoTime()
    val result  = run(spark, dataDir, query, strategy).cache()
    val groups  = result.count()
    val total   = result.agg(sum("total")).head().getDouble(0)
    val elapsed = (System.nanoTime() - t0) / 1e9

    println(f"query         = $query")
    println(f"strategy      = $strategy")
    println(f"groups        = $groups")
    println(f"total (check) = $total%.1f")
    println(f"elapsed       = $elapsed%.2f s")

    spark.stop()
  }
}
