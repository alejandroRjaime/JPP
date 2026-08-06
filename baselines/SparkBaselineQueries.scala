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
    // Three strategies, differing only in how the join is allowed to be planned:
    //
    //   smj   threshold pinned to -1, no hints. Broadcasting is disabled outright
    //         so that the Sort-Merge Join baseline cannot be silently converted
    //         into a broadcast plan.
    //   bhj   threshold pinned to -1, every dimension force-broadcast by hint.
    //         The hint is what selects the plan; the threshold is neutralised so
    //         that it cannot also contribute.
    //   auto  threshold left exactly as the session received it, no hints. This
    //         is the configuration a reviewer asks about: the optimizer decides,
    //         and the decision is a function of the threshold being swept.
    //
    // `auto` must not call `conf.set` at all — overwriting the value here would
    // silently discard the sweep's --conf and make every point of the sweep the
    // same measurement.
    val strat = strategy.toLowerCase
    require(Set("smj", "bhj", "auto").contains(strat), s"unknown strategy: $strategy")
    if (strat != "auto") spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val bcast = strat == "bhj"
    def d(df: DataFrame): DataFrame = if (bcast) broadcast(df) else df

    val lo = spark.read.parquet(s"$dataDir/lineorder")

    // Same guard as the pattern side. Q5 reads a different fact table and is
    // checked inside its own branch; the dimensions are checked here, before the
    // projections, because the projection does not change the partitioning and
    // checking once keeps the guard out of five near-identical branches.
    if (query.toLowerCase != "q5") {
      PartitionGuard.check(
        Seq("lineorder" -> lo,
            "customer"  -> spark.read.parquet(s"$dataDir/customer"),
            "supplier"  -> spark.read.parquet(s"$dataDir/supplier"),
            "part"      -> spark.read.parquet(s"$dataDir/part"),
            "date"      -> spark.read.parquet(s"$dataDir/date")),
        s"SparkBaselineQueries/$query/$strat")
    }

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

      case "q5" =>
        // Q1 restricted to one year over the year-partitioned fact table.
        // The predicate is on the partitioning key, so the scan prunes; the
        // joins that follow still redistribute whatever survives.
        val loPart = spark.read.parquet(s"$dataDir/lineorder_by_year")
          .filter(col("lo_year") === 1997)
        val c = spark.read.parquet(s"$dataDir/customer").select("c_custkey", "c_nation")
        val s2 = spark.read.parquet(s"$dataDir/supplier").select("s_suppkey")
        val p = spark.read.parquet(s"$dataDir/part").select("p_partkey")
        val t = spark.read.parquet(s"$dataDir/date").select("d_datekey", "d_year")
        loPart.join(d(c),  loPart("lo_custkey")   === c("c_custkey"))
              .join(d(s2), loPart("lo_suppkey")   === s2("s_suppkey"))
              .join(d(p),  loPart("lo_partkey")   === p("p_partkey"))
              .join(d(t),  loPart("lo_orderdate") === t("d_datekey"))
              .groupBy("d_year", "c_nation").agg(sum("amount").alias("total"))

      case other =>
        throw new IllegalArgumentException(s"unknown query: $other (expected q1..q5)")
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

    // Installed before any action so that no execution escapes the record.
    RunReport.install(spark, sys.env.getOrElse("JPP_RUN_LABEL", s"$query-$strategy"))

    val t0      = System.nanoTime()
    val result  = run(spark, dataDir, query, strategy).cache()
    val groups  = result.count()
    val total   = result.agg(sum("total")).head().getDouble(0)
    val elapsed = (System.nanoTime() - t0) / 1e9

    println(f"query         = $query")
    println(f"strategy      = $strategy")
    // The value in force during the run, read back rather than assumed: for
    // `auto` it comes from the sweep's --conf, and reporting the intended value
    // instead of the effective one is exactly the gap the reviewer identified.
    println(f"threshold     = ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")
    println(f"groups        = $groups")
    println(f"total (check) = $total%.1f")
    println(f"elapsed       = $elapsed%.2f s")

    RunReport.emit(spark)
    spark.stop()
  }
}
