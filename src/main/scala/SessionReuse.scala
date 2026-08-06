// =============================================================================
// Does the pattern's broadcast cost amortise across a session?
//
// The paper argues that the pattern broadcasts once per session while a
// relational plan rebuilds and rebroadcasts per query. Nothing in this
// repository supported that: every benchmark runs one query per JVM, so a
// per-session saving cannot appear in any of the numbers. This measures it.
//
// Both regimes execute Q1-Q4 inside ONE SparkSession:
//
//   joinless    Phase 1 built once for the structures the queries share, then
//               reused. Query-specific structures are built when first needed
//               and are also reused if a later query needs the same one.
//   relational  the same four queries as join plans at the default threshold,
//               written the way a user would write them.
//
// The honest complication, reported rather than smoothed over: the four queries
// do not all want the same structures. Q2 needs a `customer` array filtered to
// one region and a `supplier` flag array filtered likewise; Q3 needs a `part`
// dictionary. So "broadcast once per session" holds for what is shared and not
// for what is not, and the breakdown below separates the two.
//
// The relational side is given the same treatment a user would get from Spark:
// each query builds its own DataFrames. Spark caches a broadcast per
// BroadcastExchange instance, and separate queries produce separate instances,
// so the dimensions are rebuilt per query. That is the behaviour under test.
//
// Usage:  ./scripts/jpp SessionReuse <dataDir> <joinless|relational> [csvOut]
// =============================================================================

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import scala.collection.mutable

object SessionReuse {

  private def now(): Long = System.nanoTime()
  private def secs(t0: Long): Double = (now() - t0) / 1e9

  final case class QueryResult(query: String, elapsed: Double, phase1: Double,
                               groups: Long, total: Double, bcastMs: Long)

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.nonEmpty)   args(0) else "/home/athenas/ssb_synth"
    val regime  = if (args.length > 1) args(1) else "joinless"
    val csvOut  = if (args.length > 2) args(2) else "results/session_reuse.csv"
    val cores   = if (args.length > 3) args(3) else "16"
    require(Set("joinless", "relational", "relational-cached", "relational-cached-hi").contains(regime),
      s"unknown regime: $regime")

    val spark = SparkSession.builder()
      .appName(s"SessionReuse-$regime")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      // The pattern pins the threshold off, as it does everywhere else. The
      // relational regime is left at Spark's default, which the sweep showed
      // already broadcasts all four dimensions at this scale.
      // `relational-cached-hi` is the fair form of the caching objection. Caching
      // inflates the optimizer's estimate of a dimension by 1.8x to 7.3x, because
      // an InMemoryRelation is measured in memory rather than as compressed bytes
      // on disk, and that pushes `customer` (57.6 MB cached) and `part` (11.2 MB)
      // past the 10 MiB default. Left at the default, caching therefore *removes*
      // broadcasts rather than making them cheaper, and the comparison would be
      // measuring a plan change instead of the effect of caching. 100 MiB clears
      // every cached estimate, so this regime keeps all four broadcasts.
      .config("spark.sql.autoBroadcastJoinThreshold", regime match {
        case "joinless"             => "-1"
        case "relational-cached-hi" => "104857600"
        case _                      => "10485760"
      })
      .getOrCreate()

    RunReport.install(spark, sys.env.getOrElse("JPP_RUN_LABEL", s"session-$regime"))

    val results = mutable.ArrayBuffer.empty[QueryResult]
    val sessionT0 = now()

    regime match {
      case "joinless"          => runJoinless(spark, dataDir, results)
      case "relational"        => runRelational(spark, dataDir, results)
      case "relational-cached" | "relational-cached-hi" => runRelationalCached(spark, dataDir, results)
    }

    val sessionTotal = secs(sessionT0)

    // --- report ---------------------------------------------------------------
    println()
    println(f"regime        = $regime")
    println(f"session total = $sessionTotal%.2f s")
    val phase1Total = results.map(_.phase1).sum
    val bcastTotal  = results.map(_.bcastMs).sum
    println(f"phase1 total  = $phase1Total%.2f s   (structure building, cumulative)")
    println(f"bcast total   = $bcastTotal ms  (broadcast exchanges, cumulative)")
    results.foreach { r =>
      println(f"  ${r.query}%-4s elapsed=${r.elapsed}%6.2f s  phase1=${r.phase1}%5.2f s  " +
              f"groups=${r.groups}%4d  total=${r.total}%,18.1f  bcast=${r.bcastMs}%6d ms")
    }

    new File(csvOut).getAbsoluteFile.getParentFile.mkdirs()
    val exists = new File(csvOut).exists()
    val w = new PrintWriter(new java.io.FileWriter(csvOut, true))
    try {
      if (!exists) w.println("regime,query,elapsed_s,phase1_s,groups,total,bcast_ms,session_total_s")
      // Locale.ROOT: this host formats decimals with a comma, which in a
      // comma-separated file splits one field into two and shifts every column
      // after it. The f-interpolator uses the default locale, so the numbers are
      // formatted explicitly here instead.
      def d(x: Double, places: Int) =
        String.format(java.util.Locale.ROOT, s"%.${places}f", Double.box(x))
      results.foreach { r =>
        w.println(s"$regime,${r.query},${d(r.elapsed, 3)},${d(r.phase1, 3)},${r.groups},"
                + s"${d(r.total, 1)},${r.bcastMs},${d(sessionTotal, 3)}")
      }
    } finally w.close()
    println(s"\nappended to $csvOut")

    RunReport.emit(spark)
    spark.stop()
  }

  // ---------------------------------------------------------------------------
  private def runJoinless(spark: SparkSession, dataDir: String,
                          out: mutable.ArrayBuffer[QueryResult]): Unit = {
    import JoinlessPartitionPattern.{buildIntArrayAuto => buildIntArray,
                                     buildFlagArrayAuto => buildFlagArray}
    val sc   = spark.sparkContext
    val cust = spark.read.parquet(s"$dataDir/customer")
    val supp = spark.read.parquet(s"$dataDir/supplier")
    val part = spark.read.parquet(s"$dataDir/part")
    val dat  = spark.read.parquet(s"$dataDir/date")
    val lo   = spark.read.parquet(s"$dataDir/lineorder")

    // Memoised structures. A structure is built the first time a query needs it
    // and reused afterwards — which is the mechanism the paper's claim rests on,
    // made explicit rather than assumed.
    val cache = mutable.Map.empty[String, Any]
    var phase1Accum = 0.0
    def shared[T](key: String)(build: => T): T = {
      cache.get(key) match {
        case Some(v) => v.asInstanceOf[T]
        case None =>
          val t = now()
          val v = build
          phase1Accum += secs(t)
          cache(key) = v
          v
      }
    }

    def run(q: String)(body: () => DataFrame): Unit = {
      val before = phase1Accum
      val t0 = now()
      val df = body().cache()
      val groups = df.count()
      val total  = df.agg(sum("total")).head().getDouble(0)
      val e = secs(t0)
      out += QueryResult(q, e, phase1Accum - before, groups, total, 0L)
      df.unpersist()
    }

    def custNation() = shared("custNation")(sc.broadcast(buildIntArray(cust, "c_custkey", "c_nation")._1))
      .asInstanceOf[Broadcast[Array[Int]]]
    def dateYear()   = shared("dateYear")(sc.broadcast(buildIntArray(dat, "d_datekey", "d_year")._1))
      .asInstanceOf[Broadcast[Array[Int]]]
    def suppOk()     = shared("suppOk")(sc.broadcast(buildFlagArray(supp, "s_suppkey")))
      .asInstanceOf[Broadcast[Array[Boolean]]]
    def partOk()     = shared("partOk")(sc.broadcast(buildFlagArray(part, "p_partkey")))
      .asInstanceOf[Broadcast[Array[Boolean]]]

    run("q1")(() => JoinlessKernels.yearNation(lo, custNation(), dateYear(), Some(suppOk()), Some(partOk())))

    run("q2") { () =>
      val cn = shared("custNationAmerica")(sc.broadcast(
        buildIntArray(cust.filter(col("c_region") === "AMERICA"), "c_custkey", "c_nation")._1))
        .asInstanceOf[Broadcast[Array[Int]]]
      val so = shared("suppOkAmerica")(sc.broadcast(
        buildFlagArray(supp, "s_suppkey", Some(col("s_region") === "AMERICA"))))
        .asInstanceOf[Broadcast[Array[Boolean]]]
      JoinlessKernels.yearNation(lo, cn, dateYear(), Some(so), Some(partOk()))
    }

    run("q3") { () =>
      val pm = shared("partMfgr")(sc.broadcast(
        buildIntArray(part.filter(col("p_mfgr").isin("MFGR#1", "MFGR#2")), "p_partkey", "p_mfgr")._1))
        .asInstanceOf[Broadcast[Array[Int]]]
      JoinlessKernels.yearNationMfgr(lo, custNation(), dateYear(), pm, suppOk())
    }

    run("q4")(() => JoinlessKernels.yearNation(lo, custNation(), dateYear(), None, None))
  }

  // ---------------------------------------------------------------------------
  private def runRelational(spark: SparkSession, dataDir: String,
                            out: mutable.ArrayBuffer[QueryResult]): Unit = {
    // Broadcast cost is read from RunReport's listener, which sees the executed
    // plan of every query in this session. The delta between queries is the cost
    // that query paid, which is the quantity the paper's claim is about.
    var seenMs = 0L
    def run(q: String): Unit = {
      val t0 = now()
      val df = SparkBaselineQueries.run(spark, dataDir, q, "auto").cache()
      val groups = df.count()
      val total  = df.agg(sum("total")).head().getDouble(0)
      val e = secs(t0)
      val nowMs = RunReport.cumulativeBroadcastMs
      out += QueryResult(q, e, 0.0, groups, total, nowMs - seenMs)
      seenMs = nowMs
      df.unpersist()
    }
    Seq("q1", "q2", "q3", "q4").foreach(run)
  }

  // ---------------------------------------------------------------------------
  /** The relational regime with the dimensions cached and materialised up front.
    *
    * This is the obvious objection to the previous regime, and it is measured
    * rather than argued: cache the dimensions and the per-query cost of reading
    * them disappears. What that does *not* obviously do is remove the broadcast,
    * because a `BroadcastExchange` still has to build the relation and ship it,
    * and separate queries produce separate exchange instances.
    *
    * The caching works through Spark's CacheManager, which substitutes a cached
    * subtree wherever a later plan contains a matching one. The dimensions are
    * cached as the full `read.parquet(...)` that SparkBaselineQueries itself
    * starts from, so its projections sit on top of the cached scan rather than
    * re-reading the files. That the substitution actually happened is visible in
    * the plan files RunReport writes, as InMemoryTableScan under each exchange.
    *
    * The materialisation cost is recorded as this regime's Phase 1, so that the
    * three regimes carry comparable columns: it is a fixed cost paid once per
    * session before any query, which is exactly what Phase 1 is for the pattern.
    */
  private def runRelationalCached(spark: SparkSession, dataDir: String,
                                  out: mutable.ArrayBuffer[QueryResult]): Unit = {
    val t0 = now()
    val dims = Seq("customer", "supplier", "part", "date")
      .map(n => n -> spark.read.parquet(s"$dataDir/$n"))
    dims.foreach { case (_, df) => df.cache() }
    // Materialised before the first query, so no query pays for filling the
    // cache and the setup cost is attributable.
    val cachedRows = dims.map { case (_, df) => df.count() }.sum
    val setup = secs(t0)
    println(f"cached dimensions: ${dims.size} tables, $cachedRows%,d rows, $setup%.2f s")

    var seenMs = 0L
    var first = true
    def run(q: String): Unit = {
      val t = now()
      val df = SparkBaselineQueries.run(spark, dataDir, q, "auto").cache()
      val groups = df.count()
      val total  = df.agg(sum("total")).head().getDouble(0)
      val e = secs(t)
      val nowMs = RunReport.cumulativeBroadcastMs
      // The materialisation is charged to the first query, so the per-regime
      // totals include it exactly once rather than sitting outside the table.
      out += QueryResult(q, e, if (first) setup else 0.0, groups, total, nowMs - seenMs)
      first = false
      seenMs = nowMs
      df.unpersist()
    }
    Seq("q1", "q2", "q3", "q4").foreach(run)
    dims.foreach { case (_, df) => df.unpersist() }
  }
}
