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

  import JoinlessPartitionPattern.{buildIntArray, buildFlagArray}

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

  // ---------------------------------------------------------------------------
  // Q1, Q2 and Q4 share a shape: group by (d_year, c_nation), optionally with
  // supplier/part probes acting as filters. Q4 simply omits them.
  // ---------------------------------------------------------------------------
  private def runYearNation(
      factDF: DataFrame,
      custNation: Broadcast[Array[Int]],
      dateYear: Broadcast[Array[Int]],
      suppOk: Option[Broadcast[Array[Boolean]]],
      partOk: Option[Broadcast[Array[Boolean]]]
  ): DataFrame = {

    val spark  = factDF.sparkSession
    val schema = StructType(Seq(
      StructField("d_year",      IntegerType, nullable = false),
      StructField("c_nation",    IntegerType, nullable = false),
      StructField("partial_agg", DoubleType,  nullable = false)
    ))

    // Project before converting to RDD. `Dataset.rdd` materializes whatever the
    // plan produces, so without an explicit projection the scan reads every
    // column of the fact table regardless of which ones the pass touches. On
    // local NVMe that is invisible; over a storage link it is paid in full.
    val needed = Seq("lo_custkey", "lo_orderdate", "amount") ++
      suppOk.map(_ => "lo_suppkey") ++ partOk.map(_ => "lo_partkey")
    val projected = factDF.select(needed.map(col): _*)

    val fs = projected.schema
    requireTypes(fs, needed.filterNot(_ == "amount"), "amount")
    val iCust = fs.fieldIndex("lo_custkey")
    val iDate = fs.fieldIndex("lo_orderdate")
    val iAmt  = fs.fieldIndex("amount")
    val iSupp = suppOk.map(_ => fs.fieldIndex("lo_suppkey")).getOrElse(-1)
    val iPart = partOk.map(_ => fs.fieldIndex("lo_partkey")).getOrElse(-1)

    val partials = projected.rdd.mapPartitions { iter =>
      val cn = custNation.value
      val dy = dateYear.value
      val so = suppOk.map(_.value).orNull
      val po = partOk.map(_.value).orNull
      val acc = new mutable.LongMap[Double]()

      while (iter.hasNext) {
        val row = iter.next()
        val ck  = row.getLong(iCust).toInt
        val dk  = row.getLong(iDate).toInt

        if (ck >= 0 && ck < cn.length && dk >= 0 && dk < dy.length) {
          // Predicates are resolved by the same array probe that would perform
          // the lookup: a non-qualifying row costs one bounds-checked read.
          val passSupp = (so eq null) || { val k = row.getLong(iSupp).toInt; k >= 0 && k < so.length && so(k) }
          val passPart = (po eq null) || { val k = row.getLong(iPart).toInt; k >= 0 && k < po.length && po(k) }

          if (passSupp && passPart) {
            val nation = cn(ck)
            val year   = dy(dk)
            if (nation >= 0 && year >= 0) {
              val key = year.toLong * NATION_RADIX + nation.toLong
              acc.update(key, acc.getOrElse(key, 0.0) + row.getDouble(iAmt))
            }
          }
        }
      }
      acc.iterator.map { case (k, v) =>
        Row((k / NATION_RADIX).toInt, (k % NATION_RADIX).toInt, v)
      }
    }

    spark.createDataFrame(partials, schema)
      .groupBy("d_year", "c_nation").agg(sum("partial_agg").alias("total"))
  }

  // ---------------------------------------------------------------------------
  // Q3: three group-by attributes. The manufacturer both filters (values not in
  // the selected set map to -1) and contributes to the group key, so the packing
  // gains a third field.
  // ---------------------------------------------------------------------------
  private def runYearNationMfgr(
      factDF: DataFrame,
      custNation: Broadcast[Array[Int]],
      dateYear: Broadcast[Array[Int]],
      partMfgr: Broadcast[Array[Int]],
      suppOk: Broadcast[Array[Boolean]]
  ): DataFrame = {

    val spark  = factDF.sparkSession
    val schema = StructType(Seq(
      StructField("d_year",      IntegerType, nullable = false),
      StructField("c_nation",    IntegerType, nullable = false),
      StructField("p_mfgr",      IntegerType, nullable = false),
      StructField("partial_agg", DoubleType,  nullable = false)
    ))

    val needed = Seq("lo_custkey", "lo_suppkey", "lo_partkey", "lo_orderdate", "amount")
    val projected = factDF.select(needed.map(col): _*)

    val fs = projected.schema
    requireTypes(fs, needed.filterNot(_ == "amount"), "amount")
    val iCust = fs.fieldIndex("lo_custkey")
    val iSupp = fs.fieldIndex("lo_suppkey")
    val iPart = fs.fieldIndex("lo_partkey")
    val iDate = fs.fieldIndex("lo_orderdate")
    val iAmt  = fs.fieldIndex("amount")

    val partials = projected.rdd.mapPartitions { iter =>
      val cn = custNation.value
      val dy = dateYear.value
      val pm = partMfgr.value
      val so = suppOk.value
      val acc = new mutable.LongMap[Double]()

      while (iter.hasNext) {
        val row = iter.next()
        val ck = row.getLong(iCust).toInt
        val sk = row.getLong(iSupp).toInt
        val pk = row.getLong(iPart).toInt
        val dk = row.getLong(iDate).toInt

        if (ck >= 0 && ck < cn.length && sk >= 0 && sk < so.length &&
            pk >= 0 && pk < pm.length && dk >= 0 && dk < dy.length && so(sk)) {
          val mfgr = pm(pk)          // -1 for manufacturers outside the predicate
          if (mfgr >= 0) {
            val nation = cn(ck)
            val year   = dy(dk)
            if (nation >= 0 && year >= 0) {
              val key = (year.toLong * NATION_RADIX + nation.toLong) * MFGR_RADIX + mfgr.toLong
              acc.update(key, acc.getOrElse(key, 0.0) + row.getDouble(iAmt))
            }
          }
        }
      }
      acc.iterator.map { case (k, v) =>
        val mfgr = (k % MFGR_RADIX).toInt
        val rest = k / MFGR_RADIX
        Row((rest / NATION_RADIX).toInt, (rest % NATION_RADIX).toInt, mfgr, v)
      }
    }

    spark.createDataFrame(partials, schema)
      .groupBy("d_year", "c_nation", "p_mfgr").agg(sum("partial_agg").alias("total"))
  }

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

    val t0       = System.nanoTime()
    val prepared = run(spark, dataDir, query, repart)
    val result   = prepared.df.cache()
    val groups   = result.count()
    val total    = result.agg(sum("total")).head().getDouble(0)
    val elapsed  = (System.nanoTime() - t0) / 1e9
    val setup    = prepared.setupSeconds

    println(f"query         = $query")
    println(f"repartition   = ${repart.map(_.toString).getOrElse("none")}")
    println(f"setup         = $setup%.2f s")
    println(f"scan          = ${elapsed - setup}%.2f s")
    println(f"groups        = $groups")
    println(f"total (check) = $total%.1f")
    println(f"elapsed       = $elapsed%.2f s")

    spark.stop()
  }
}
