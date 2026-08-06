// =============================================================================
// The two partition-local kernels of the pattern, in one place.
//
// They were private to JoinlessQueries. SessionReuse needs the same kernels, and
// the one thing that must not happen is a second copy: these loops are what the
// paper measures, so a duplicate that drifted by a line would make two sets of
// numbers in the same report incomparable while looking identical.
//
// Phase 3 — the terminal shuffle — is included here because it is part of the
// same measured region; only Phase 1 differs between callers.
// =============================================================================

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable

object JoinlessKernels {

  // Radices for the packed group key. Each must strictly exceed the cardinality
  // of the attribute it encodes, or the packing is not injective.
  final val NATION_RADIX = 1000L
  final val MFGR_RADIX   = 100L

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
  def yearNation(
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
  def yearNationMfgr(
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
}
