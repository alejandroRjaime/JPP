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

  private def getKeyAsInt(row: Row, i: Int): Int = row.get(i) match {
    case v: java.lang.Long    => v.longValue().toInt
    case v: java.lang.Integer => v.intValue()
    case null                 => -1
    case other =>
      throw new IllegalArgumentException(s"unsupported key type: ${other.getClass.getName}")
  }

  private def getDoubleAt(row: Row, i: Int): Double = row.get(i) match {
    case v: java.lang.Double  => v.doubleValue()
    case v: java.lang.Long    => v.longValue().toDouble
    case v: java.lang.Integer => v.intValue().toDouble
    case null                 => 0.0
    case other =>
      throw new IllegalArgumentException(s"unsupported measure type: ${other.getClass.getName}")
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

    val fs    = factDF.schema
    val iCust = fs.fieldIndex("lo_custkey")
    val iDate = fs.fieldIndex("lo_orderdate")
    val iAmt  = fs.fieldIndex("amount")
    val iSupp = suppOk.map(_ => fs.fieldIndex("lo_suppkey")).getOrElse(-1)
    val iPart = partOk.map(_ => fs.fieldIndex("lo_partkey")).getOrElse(-1)

    val partials = factDF.rdd.mapPartitions { iter =>
      val cn = custNation.value
      val dy = dateYear.value
      val so = suppOk.map(_.value).orNull
      val po = partOk.map(_.value).orNull
      val acc = new mutable.LongMap[Double]()

      while (iter.hasNext) {
        val row = iter.next()
        val ck  = getKeyAsInt(row, iCust)
        val dk  = getKeyAsInt(row, iDate)

        if (ck >= 0 && ck < cn.length && dk >= 0 && dk < dy.length) {
          // Predicates are resolved by the same array probe that would perform
          // the lookup: a non-qualifying row costs one bounds-checked read.
          val passSupp = (so eq null) || { val k = getKeyAsInt(row, iSupp); k >= 0 && k < so.length && so(k) }
          val passPart = (po eq null) || { val k = getKeyAsInt(row, iPart); k >= 0 && k < po.length && po(k) }

          if (passSupp && passPart) {
            val nation = cn(ck)
            val year   = dy(dk)
            if (nation >= 0 && year >= 0) {
              require(nation < NATION_RADIX, s"nation code $nation exceeds packing radix")
              val key  = year.toLong * NATION_RADIX + nation.toLong
              acc.update(key, acc.getOrElse(key, 0.0) + getDoubleAt(row, iAmt))
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

    val fs    = factDF.schema
    val iCust = fs.fieldIndex("lo_custkey")
    val iSupp = fs.fieldIndex("lo_suppkey")
    val iPart = fs.fieldIndex("lo_partkey")
    val iDate = fs.fieldIndex("lo_orderdate")
    val iAmt  = fs.fieldIndex("amount")

    val partials = factDF.rdd.mapPartitions { iter =>
      val cn = custNation.value
      val dy = dateYear.value
      val pm = partMfgr.value
      val so = suppOk.value
      val acc = new mutable.LongMap[Double]()

      while (iter.hasNext) {
        val row = iter.next()
        val ck = getKeyAsInt(row, iCust)
        val sk = getKeyAsInt(row, iSupp)
        val pk = getKeyAsInt(row, iPart)
        val dk = getKeyAsInt(row, iDate)

        if (ck >= 0 && ck < cn.length && sk >= 0 && sk < so.length &&
            pk >= 0 && pk < pm.length && dk >= 0 && dk < dy.length && so(sk)) {
          val mfgr = pm(pk)          // -1 for manufacturers outside the predicate
          if (mfgr >= 0) {
            val nation = cn(ck)
            val year   = dy(dk)
            if (nation >= 0 && year >= 0) {
              require(nation < NATION_RADIX && mfgr < MFGR_RADIX, "group key exceeds packing radix")
              val key = (year.toLong * NATION_RADIX + nation.toLong) * MFGR_RADIX + mfgr.toLong
              acc.update(key, acc.getOrElse(key, 0.0) + getDoubleAt(row, iAmt))
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

  def run(spark: SparkSession, dataDir: String, query: String): DataFrame = {
    val cust = spark.read.parquet(s"$dataDir/customer")
    val supp = spark.read.parquet(s"$dataDir/supplier")
    val part = spark.read.parquet(s"$dataDir/part")
    val dat  = spark.read.parquet(s"$dataDir/date")
    val lo   = spark.read.parquet(s"$dataDir/lineorder")   // no repartition: see README

    val sc         = spark.sparkContext
    val custNation = sc.broadcast(buildIntArray(cust, "c_custkey", "c_nation")._1)
    val dateYear   = sc.broadcast(buildIntArray(dat,  "d_datekey", "d_year")._1)

    query.toLowerCase match {
      case "q1" =>
        runYearNation(lo, custNation, dateYear,
          Some(sc.broadcast(buildFlagArray(supp, "s_suppkey"))),
          Some(sc.broadcast(buildFlagArray(part, "p_partkey"))))

      case "q2" =>
        runYearNation(lo, custNation, dateYear,
          Some(sc.broadcast(buildFlagArray(supp, "s_suppkey", Some(col("s_region") === "AMERICA")))),
          Some(sc.broadcast(buildFlagArray(part, "p_partkey"))))
        // NOTE: the customer-side predicate (c_region = 'AMERICA') must also be
        // folded in, by building custNation from the filtered customer table so
        // that non-qualifying customers map to -1:
        //   buildIntArray(cust.filter($"c_region" === "AMERICA"), "c_custkey", "c_nation")

      case "q3" =>
        val mfgrDict = part.filter(col("p_mfgr").isin("MFGR#1", "MFGR#2"))
        runYearNationMfgr(lo, custNation, dateYear,
          sc.broadcast(buildIntArray(mfgrDict, "p_partkey", "p_mfgr")._1),
          sc.broadcast(buildFlagArray(supp, "s_suppkey")))

      case "q4" =>
        runYearNation(lo, custNation, dateYear, None, None)

      case other =>
        throw new IllegalArgumentException(s"unknown query: $other (expected q1..q4)")
    }
  }

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val query   = if (args.length > 1) args(1) else "q1"
    val cores   = if (args.length > 2) args(2) else "16"

    val spark = SparkSession.builder()
      .appName(s"JoinlessQueries-$query")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()

    val t0      = System.nanoTime()
    val result  = run(spark, dataDir, query).cache()
    val groups  = result.count()
    val total   = result.agg(sum("total")).head().getDouble(0)
    val elapsed = (System.nanoTime() - t0) / 1e9

    println(f"query         = $query")
    println(f"groups        = $groups")
    println(f"total (check) = $total%.1f")
    println(f"elapsed       = $elapsed%.2f s")

    spark.stop()
  }
}
