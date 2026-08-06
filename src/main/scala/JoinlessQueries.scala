// =============================================================================
// The Joinless Partition Pattern over the four-query benchmark set.
//
//   Q1  4 dimensions, no predicates          base case, high aggregation ratio
//   Q2  4 dimensions, 2 predicates           selective dimension filters
//   Q3  4 dimensions, 3 group-by attributes  reduced aggregation ratio
//   Q4  2 dimensions                         dependence on N
//
// All four are the same three-phase pattern; they differ only in which
// dimensions are probed, which predicates are folded into the lookup
// structures, and how the group key is packed.
//
// Usage:  spark-submit --class JoinlessQueries <jar> <dataDir> <q1|q2|q3|q4>
// =============================================================================

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable

object JoinlessQueries {

  // The `Auto` variants dispatch on JPP_PHASE1, so the same query code runs
  // over either Phase 1 implementation and the two are directly comparable.
  import JoinlessPartitionPattern.{buildIntArrayAuto => buildIntArray,
                                   buildFlagArrayAuto => buildFlagArray}

  // Radices for the packed group key. Each must strictly exceed the cardinality
  // of the attribute it encodes, or the packing is not injective.
  private final val NATION_RADIX = 1000L
  private final val MFGR_RADIX   = 100L

  /** Year selected by Q5. Any of the eight years in the generated date
    * dimension; the choice is arbitrary because the generator distributes
    * orders uniformly over the range. */
  final val PRUNE_YEAR = 1997

  // The per-row loops below use typed accessors rather than a match on the
  // boxed value. The tolerant accessors belong in the dimension-building code,
  // which runs once on the driver over small inputs; here they would cost
  // several dispatches per fact tuple over hundreds of millions of rows.
  private def requireTypes(schema: StructType, keys: Seq[String], measure: String): Unit = {
    keys.foreach(c => require(schema(c).dataType == LongType, s"$c must be LongType"))
    require(schema(measure).dataType == DoubleType, s"$measure must be DoubleType")
  }

  // The kernels live in JoinlessKernels so that SessionReuse runs the same code.
  // A second copy here would make the two sets of measurements incomparable
  // while looking identical.
  private def runYearNation(
      factDF: DataFrame,
      custNation: Broadcast[Array[Int]],
      dateYear: Broadcast[Array[Int]],
      suppOk: Option[Broadcast[Array[Boolean]]],
      partOk: Option[Broadcast[Array[Boolean]]]
  ): DataFrame = JoinlessKernels.yearNation(factDF, custNation, dateYear, suppOk, partOk)

  private def runYearNationMfgr(
      factDF: DataFrame,
      custNation: Broadcast[Array[Int]],
      dateYear: Broadcast[Array[Int]],
      partMfgr: Broadcast[Array[Int]],
      suppOk: Broadcast[Array[Boolean]]
  ): DataFrame = JoinlessKernels.yearNationMfgr(factDF, custNation, dateYear, partMfgr, suppOk)

  // ---------------------------------------------------------------------------

  /** @param repartitionFact if set, redistributes the fact table before the
    *  scan. This adds a full exchange and is NOT part of the pattern; it exists
    *  so that the effect of the fact-table partitioning can be measured
    *  directly rather than assumed. */
  /** Result of preparing and planning a query: the DataFrame, and the wall-clock
    * cost of Phase 1 alone — collecting the dimensions to the driver, encoding
    * them into the lookup structures, and broadcasting them.
    *
    * Phase 1 is a fixed cost: it depends on the size of the dimensions, not on
    * the number of fact rows subsequently scanned. Reporting it separately is
    * what allows the pattern's cost to be expressed as a constant plus a term
    * proportional to the scan, rather than as a single number whose meaning
    * changes with the workload. */
  case class Prepared(df: DataFrame, setupSeconds: Double)

  def run(spark: SparkSession, dataDir: String, query: String,
          repartitionFact: Option[Int] = None): Prepared = {
    val cust = spark.read.parquet(s"$dataDir/customer")
    val supp = spark.read.parquet(s"$dataDir/supplier")
    val part = spark.read.parquet(s"$dataDir/part")
    val dat  = spark.read.parquet(s"$dataDir/date")
    val loRaw = spark.read.parquet(s"$dataDir/lineorder")
    val lo = repartitionFact.fold(loRaw) { n =>
      Console.err.println(s"[WARN] repartitioning the fact table to $n partitions " +
        "adds a full exchange to the measured plan")
      loRaw.repartition(n)
    }
    println(s"fact partitions = ${lo.rdd.getNumPartitions}")

    // Partitioning is part of the experiment, so it is checked rather than
    // reported. The fact table is registered under the name it is pinned by; the
    // repartitioned variant is deliberately given a different name, because its
    // partition count is supposed to differ and pinning it to the on-disk value
    // would fail every run of the repartition experiment.
    PartitionGuard.check(
      Seq(
        (if (repartitionFact.isDefined) "lineorder_repartitioned" else "lineorder") -> lo,
        "customer" -> cust, "supplier" -> supp, "part" -> part, "date" -> dat),
      s"JoinlessQueries/$query")

    val sc     = spark.sparkContext
    val setup0 = System.nanoTime()

    val custNationA = buildIntArray(cust, "c_custkey", "c_nation")._1
    val dateYearA   = buildIntArray(dat,  "d_datekey", "d_year")._1

    // Validate the packing bounds once against the dimension arrays, before the
    // scan. Checking inside the loop would verify, once per fact tuple, a
    // property of a broadcast structure that cannot change during the scan.
    require(custNationA.isEmpty || custNationA.max < NATION_RADIX,
      s"c_nation code ${custNationA.max} exceeds packing radix $NATION_RADIX")

    val custNation = sc.broadcast(custNationA)
    val dateYear   = sc.broadcast(dateYearA)

    // Phase 1 for the two dimensions every query needs. The per-query flag and
    // dictionary arrays built below are also Phase 1 and are added to the total.
    val setupBase = System.nanoTime() - setup0
    var setupExtra = 0L
    def timed[T](f: => T): T = {
      val t = System.nanoTime(); val r = f; setupExtra += System.nanoTime() - t; r
    }

    val df = query.toLowerCase match {
      case "q1" =>
        runYearNation(lo, custNation, dateYear,
          Some(timed(sc.broadcast(buildFlagArray(supp, "s_suppkey")))),
          Some(timed(sc.broadcast(buildFlagArray(part, "p_partkey")))))

      case "q2" =>
        // Both predicates are folded into the lookup structures. Customers
        // outside the region are absent from the filtered table, so their slots
        // stay at the -1 fill value and the fact row is discarded by the probe.
        val custNationFiltered = timed(sc.broadcast(
          buildIntArray(cust.filter(col("c_region") === "AMERICA"), "c_custkey", "c_nation")._1))
        runYearNation(lo, custNationFiltered, dateYear,
          Some(timed(sc.broadcast(buildFlagArray(supp, "s_suppkey", Some(col("s_region") === "AMERICA"))))),
          Some(timed(sc.broadcast(buildFlagArray(part, "p_partkey")))))

      case "q3" =>
        val mfgrDict = part.filter(col("p_mfgr").isin("MFGR#1", "MFGR#2"))
        val partMfgrA = timed(buildIntArray(mfgrDict, "p_partkey", "p_mfgr")._1)
        require(partMfgrA.isEmpty || partMfgrA.max < MFGR_RADIX,
          s"p_mfgr code ${partMfgrA.max} exceeds packing radix $MFGR_RADIX")
        runYearNationMfgr(lo, custNation, dateYear,
          timed(sc.broadcast(partMfgrA)),
          timed(sc.broadcast(buildFlagArray(supp, "s_suppkey"))))

      case "q4" =>
        runYearNation(lo, custNation, dateYear, None, None)

      case "q5" =>
        // Q1 restricted to one year, over a fact table partitioned by year.
        // The predicate is on the partitioning key, so Spark prunes seven of
        // the eight directories before any row is read. Every strategy reads
        // the same pruned input; the question is whether the ratio between
        // them changes or only their absolute times.
        val loPart = spark.read.parquet(s"$dataDir/lineorder_by_year")
          .filter(col("lo_year") === PRUNE_YEAR)
        println(s"pruned partitions = ${loPart.rdd.getNumPartitions}")
        runYearNation(loPart, custNation, dateYear,
          Some(timed(sc.broadcast(buildFlagArray(supp, "s_suppkey")))),
          Some(timed(sc.broadcast(buildFlagArray(part, "p_partkey")))))

      case other =>
        throw new IllegalArgumentException(s"unknown query: $other (expected q1..q5)")
    }

    Prepared(df, (setupBase + setupExtra) / 1e9)
  }

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val query   = if (args.length > 1) args(1) else "q1"
    val cores   = if (args.length > 2) args(2) else "16"
    // Optional 4th argument: repartition the fact table to N partitions.
    val repart  = if (args.length > 3) Some(args(3).toInt) else None

    val spark = SparkSession.builder()
      .appName(s"JoinlessQueries-$query")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()

    RunReport.install(spark, sys.env.getOrElse("JPP_RUN_LABEL", s"$query-joinless"))

    val t0       = System.nanoTime()
    val prepared = run(spark, dataDir, query, repart)
    val result   = prepared.df.cache()
    val groups   = result.count()
    val total    = result.agg(sum("total")).head().getDouble(0)
    val elapsed  = (System.nanoTime() - t0) / 1e9
    val setup    = prepared.setupSeconds

    println(f"query         = $query")
    println(f"strategy      = joinless")
    println(f"repartition   = ${repart.map(_.toString).getOrElse("none")}")
    // `setup` is the pattern's Phase 1 — the dimensions collected, encoded and
    // broadcast — and is the term directly comparable to a relational plan's
    // broadcast exchanges, which RunReport reports as bcast_ms.
    println(f"setup         = $setup%.2f s")
    println(f"scan          = ${elapsed - setup}%.2f s")
    println(f"groups        = $groups")
    println(f"total (check) = $total%.1f")
    println(f"elapsed       = $elapsed%.2f s")

    RunReport.emit(spark)
    spark.stop()
  }
}
