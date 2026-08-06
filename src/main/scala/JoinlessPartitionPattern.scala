// =============================================================================
// Joinless Partition Pattern - optimized implementation
//
// Standalone port of the spark-shell script used for the reported measurements.
// Realizes the three-phase pattern:
//   1. dimensions materialized as dense primitive arrays indexed by surrogate
//      key and broadcast once,
//   2. a single partition-local pass: O(1) array probes + aggregation into a
//      primitive mutable.LongMap, group key packed into a Long,
//   3. one terminal shuffle over the compact partial aggregates.
//
// Preconditions:
//   - dense surrogate keys in 0..N (no gaps) for array-indexed lookup,
//   - group-by attributes of low cardinality, so (d_year, c_nation) fits packed
//     into a single Long. Enforced by `require` in `packKey`.
//
// Workload: SSB-style star schema. Fact table `lineorder` joined against
// customer / supplier / part / date, computing SUM(amount) by (d_year, c_nation).
//
// IMPORTANT - fact table partitioning:
//   `Dataset.repartition` performs a full exchange of the fact table and would
//   contradict the central claim of the pattern. It is therefore OFF by default
//   here. If the Parquet layout has too few files for the target parallelism,
//   compact the files on disk once, offline, rather than repartitioning inside
//   the measured region. If a repartition is used anyway, it must be reported
//   as part of the measured plan.
// =============================================================================

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable

object JoinlessPartitionPattern {

  // ---------------------------------------------------------------------------
  // Row accessors tolerant to surrogate keys stored as Int or Long (Parquet
  // writers differ; the original shell script assumed Long throughout).
  // ---------------------------------------------------------------------------
  private def getKeyAsInt(row: Row, i: Int): Int = row.get(i) match {
    case v: java.lang.Long       => v.longValue().toInt
    case v: java.lang.Integer    => v.intValue()
    case v: java.lang.Short      => v.shortValue().toInt
    case v: java.math.BigDecimal => v.intValue()
    case null                    => -1
    case other =>
      throw new IllegalArgumentException(
        s"unsupported surrogate key type: ${other.getClass.getName}")
  }

  /** The surrogate key without narrowing, so that a key beyond the Int range can
    * be detected instead of silently wrapping to a negative value. */
  private def getKeyAsLong(row: Row, i: Int): Long = row.get(i) match {
    case v: java.lang.Long       => v.longValue()
    case v: java.lang.Integer    => v.longValue()
    case v: java.lang.Short      => v.longValue()
    case v: java.math.BigDecimal => v.longValue()
    case null                    => -1L
    case other =>
      throw new IllegalArgumentException(
        s"unsupported surrogate key type: ${other.getClass.getName}")
  }

  private def getDoubleAt(row: Row, i: Int): Double = row.get(i) match {
    case v: java.lang.Double     => v.doubleValue()
    case v: java.lang.Float      => v.floatValue().toDouble
    case v: java.lang.Long       => v.longValue().toDouble
    case v: java.lang.Integer    => v.intValue().toDouble
    case v: java.math.BigDecimal => v.doubleValue()
    case null                    => 0.0
    case other =>
      throw new IllegalArgumentException(
        s"unsupported measure type: ${other.getClass.getName}")
  }

  // Group key packing. NATION_RADIX bounds the cardinality of the inner
  // attribute; if exceeded the packing is not injective and results would be
  // wrong. The bound is validated once against the dimension array before the
  // scan begins, not per row: a check inside the loop would run once per fact
  // tuple to verify a property of a broadcast structure that cannot change.
  private final val NATION_RADIX = 1000L

  private def validateRadix(arr: Array[Int], name: String): Unit = {
    val mx = if (arr.isEmpty) -1 else arr.max
    require(mx < NATION_RADIX,
      s"$name has code $mx, which exceeds the packing radix $NATION_RADIX")
  }

  // ---------------------------------------------------------------------------
  // Phase 1: build dense arrays indexed by surrogate key.
  //
  // Handles both numeric and textual attributes. Real SSB stores c_nation as a
  // name ("UNITED STATES"); the synthetic generator stores a numeric code. The
  // original `getString(1).toInt` only worked for the latter. Text values are
  // encoded with a deterministic sorted dictionary, returned so it can be
  // published alongside the results.
  // ---------------------------------------------------------------------------
  def buildIntArray(
      df: DataFrame,
      keyCol: String,
      valCol: String
  ): (Array[Int], Map[String, Int]) = {

    val rows = df.select(col(keyCol), col(valCol).cast(StringType)).collect()
    require(rows.nonEmpty, s"dimension is empty: $keyCol")

    val rawValues = rows.map(r => Option(r.getString(1)).getOrElse(""))
    val numeric   = rawValues.forall(v => v.nonEmpty && v.forall(_.isDigit))

    val dictionary: Map[String, Int] =
      if (numeric) Map.empty else rawValues.distinct.sorted.zipWithIndex.toMap

    // The key is read as a Long before it is narrowed. `getKeyAsInt` truncates,
    // and a key above 2^31 truncates to a negative value that the `k >= 0` guard
    // below then skips — so an out-of-range key would be dropped in silence and
    // the array would come out sized by the largest key that happened to fit.
    // The precondition the paper states is dense keys; this is the additional
    // bound that dense-and-large keys run into, and it is checked rather than
    // assumed.
    val maxKeyLong = rows.map(r => getKeyAsLong(r, 0)).max
    require(maxKeyLong >= 0, s"no valid surrogate keys in $keyCol")
    require(maxKeyLong < Int.MaxValue,
      s"largest surrogate key in $keyCol is $maxKeyLong, beyond the Int range an " +
      s"indexed array can address (${Int.MaxValue}). The HashMap variant applies.")
    val maxKey = maxKeyLong.toInt

    // -1 marks "no dimension row for this key", distinguishable from a real 0.
    val arr = Array.fill(maxKey + 1)(-1)

    var i = 0
    while (i < rows.length) {
      val k = getKeyAsInt(rows(i), 0)
      if (k >= 0) arr(k) = if (numeric) rawValues(i).toInt else dictionary(rawValues(i))
      i += 1
    }
    (arr, dictionary)
  }

  // ---------------------------------------------------------------------------
  // Phase 1, streaming variant: same structure, built without a driver-side
  // collect.
  //
  // `buildIntArray` above calls `collect()`, which materialises every dimension
  // row as a boxed `Row` on the driver at once. That is what fails at a 1 GB
  // dimension: 254.7M rows exceed `spark.driver.maxResultSize`, and raising the
  // limit turns the failure into a driver OOM on a 32 GB heap. The array it was
  // trying to produce is only about 1 GB and fits comfortably; the collect is
  // the problem, not the structure.
  //
  // Here the array is allocated once and filled from `toLocalIterator`, which
  // runs one job per partition and brings back one partition at a time. Peak
  // driver memory becomes the array plus a single partition, rather than the
  // array plus the whole dimension.
  //
  // Two quantities are needed before the scan and are obtained without a
  // collect: the largest key, from an aggregate, and the value dictionary, from
  // `distinct` — which returns one row per distinct attribute value (25 nations
  // here), not one per dimension row.
  // ---------------------------------------------------------------------------
  def buildIntArrayStreaming(
      df: DataFrame,
      keyCol: String,
      valCol: String
  ): (Array[Int], Map[String, Int]) = {

    val proj = df.select(col(keyCol).cast(LongType).alias("k"),
                         col(valCol).cast(StringType).alias("v"))

    val maxRow = proj.agg(max(col("k"))).head()
    require(!maxRow.isNullAt(0), s"no valid surrogate keys in $keyCol")
    val maxKey = maxRow.getLong(0)
    require(maxKey >= 0, s"no valid surrogate keys in $keyCol")
    require(maxKey < Int.MaxValue,
      s"largest key $maxKey exceeds the addressable range of an indexed array; " +
      s"the HashMap variant applies")

    // Distinct attribute values only. This is a collect, but of the value
    // domain, not of the dimension: it returns as many rows as there are
    // distinct attribute values.
    val distinctVals = proj.select(col("v")).distinct().collect()
      .map(r => Option(r.getString(0)).getOrElse(""))
    val numeric = distinctVals.forall(v => v.nonEmpty && v.forall(_.isDigit))
    val dictionary: Map[String, Int] =
      if (numeric) Map.empty else distinctVals.distinct.sorted.zipWithIndex.toMap

    val arr = Array.fill(maxKey.toInt + 1)(-1)

    val it = proj.toLocalIterator()
    while (it.hasNext) {
      val r = it.next()
      if (!r.isNullAt(0)) {
        val k = r.getLong(0)
        if (k >= 0 && k <= maxKey) {
          val raw = Option(r.getString(1)).getOrElse("")
          arr(k.toInt) = if (numeric) raw.toInt else dictionary.getOrElse(raw, -1)
        }
      }
    }
    (arr, dictionary)
  }

  /** Streaming counterpart of `buildFlagArray`. */
  def buildFlagArrayStreaming(
      df: DataFrame,
      keyCol: String,
      predicate: Option[Column] = None
  ): Array[Boolean] = {

    val keys = df.select(col(keyCol).cast(LongType).alias("k"))
    val maxRow = keys.agg(max(col("k"))).head()
    require(!maxRow.isNullAt(0), s"no valid surrogate keys in $keyCol")
    val maxKey = maxRow.getLong(0)
    require(maxKey >= 0, s"no valid surrogate keys in $keyCol")
    require(maxKey < Int.MaxValue,
      s"largest key $maxKey exceeds the addressable range of an indexed array")

    val arr = new Array[Boolean](maxKey.toInt + 1)
    val accepted = predicate.fold(df)(p => df.filter(p))
      .select(col(keyCol).cast(LongType).alias("k"))

    val it = accepted.toLocalIterator()
    while (it.hasNext) {
      val r = it.next()
      if (!r.isNullAt(0)) {
        val k = r.getLong(0)
        if (k >= 0 && k <= maxKey) arr(k.toInt) = true
      }
    }
    arr
  }

  /** Which Phase 1 implementation to use, from JPP_PHASE1 (`collect` by default,
    * `streaming` for the variant above). Selecting it by environment keeps the
    * two paths comparable: the query code, the fact-table scan and the
    * aggregation are byte-identical between the two, so a difference in the
    * measurement is a difference in Phase 1 and nothing else. */
  def phase1Mode: String = sys.env.getOrElse("JPP_PHASE1", "collect")

  def buildIntArrayAuto(df: DataFrame, keyCol: String, valCol: String): (Array[Int], Map[String, Int]) =
    if (phase1Mode == "streaming") buildIntArrayStreaming(df, keyCol, valCol)
    else buildIntArray(df, keyCol, valCol)

  def buildFlagArrayAuto(df: DataFrame, keyCol: String, predicate: Option[Column] = None): Array[Boolean] =
    if (phase1Mode == "streaming") buildFlagArrayStreaming(df, keyCol, predicate)
    else buildFlagArray(df, keyCol, predicate)

  // Flags for filter-only dimensions.
  //   predicate = None  -> presence only (every key in the dimension accepted)
  //   predicate = Some  -> an actual selective filter
  // The original code always produced presence-only flags while the comments
  // described them as filters.
  def buildFlagArray(
      df: DataFrame,
      keyCol: String,
      predicate: Option[Column] = None
  ): Array[Boolean] = {

    val all = df.select(col(keyCol)).collect()
    require(all.nonEmpty, s"dimension is empty: $keyCol")
    // Same narrowing hazard as in buildIntArray: checked before truncation.
    val maxKeyLong = all.map(r => getKeyAsLong(r, 0)).max
    require(maxKeyLong >= 0, s"no valid surrogate keys in $keyCol")
    require(maxKeyLong < Int.MaxValue,
      s"largest surrogate key in $keyCol is $maxKeyLong, beyond the Int range an " +
      s"indexed array can address (${Int.MaxValue}). The HashMap variant applies.")
    val maxKey = maxKeyLong.toInt

    val accepted = predicate.fold(df)(p => df.filter(p)).select(col(keyCol)).collect()
    val arr = new Array[Boolean](maxKey + 1)

    var i = 0
    while (i < accepted.length) {
      val k = getKeyAsInt(accepted(i), 0)
      if (k >= 0 && k <= maxKey) arr(k) = true
      i += 1
    }
    arr
  }

  // ---------------------------------------------------------------------------
  // Phases 2 and 3: partition-local aggregation, then one terminal shuffle.
  // The fact table is consumed as laid out on disk; no exchange is introduced.
  // ---------------------------------------------------------------------------
  def joinlessMax(
      factDF: DataFrame,
      custNation: Broadcast[Array[Int]],
      suppOk: Broadcast[Array[Boolean]],
      partOk: Broadcast[Array[Boolean]],
      dateYear: Broadcast[Array[Int]],
      custKeyCol: String = "lo_custkey",
      suppKeyCol: String = "lo_suppkey",
      partKeyCol: String = "lo_partkey",
      dateKeyCol: String = "lo_orderdate",
      measureCol: String = "amount"
  ): DataFrame = {

    val spark = factDF.sparkSession

    val schema = StructType(Seq(
      StructField("d_year",      IntegerType, nullable = false),
      StructField("c_nation",    IntegerType, nullable = false),
      StructField("partial_agg", DoubleType,  nullable = false)
    ))

    // Column indices resolved on the driver from the schema, not per partition
    // from the first row: the original read `iter.buffered.head`, which skipped
    // empty partitions entirely and re-resolved indices on every partition.
    // Project before converting to RDD: `Dataset.rdd` materializes whatever the
    // plan produces, so without this the scan reads every column of the fact
    // table regardless of which ones the pass touches.
    val projected = factDF.select(
      Seq(custKeyCol, suppKeyCol, partKeyCol, dateKeyCol, measureCol).map(col): _*)

    val factSchema = projected.schema
    // The hot loop reads keys as Long and the measure as Double. Verify that
    // once, here, so a schema mismatch fails with a clear message instead of a
    // ClassCastException inside a task.
    Seq(custKeyCol, suppKeyCol, partKeyCol, dateKeyCol).foreach { c =>
      require(factSchema(c).dataType == LongType, s"$c must be LongType")
    }
    require(factSchema(measureCol).dataType == DoubleType, s"$measureCol must be DoubleType")

    val iCust = factSchema.fieldIndex(custKeyCol)
    val iSupp = factSchema.fieldIndex(suppKeyCol)
    val iPart = factSchema.fieldIndex(partKeyCol)
    val iDate = factSchema.fieldIndex(dateKeyCol)
    val iAmt  = factSchema.fieldIndex(measureCol)

    validateRadix(custNation.value, "c_nation")

    val partialAggs = projected.rdd.mapPartitions { iter =>
      // Dereference the broadcast arrays once per partition.
      val cn = custNation.value
      val so = suppOk.value
      val po = partOk.value
      val dy = dateYear.value

      val localAggs = new mutable.LongMap[Double]()

      // Typed accessors, not the tolerant ones used to build the dimension
      // arrays: this loop runs once per fact tuple, so a match on the boxed
      // value would cost several dispatches per row over hundreds of millions
      // of rows. Key and measure types are fixed by the schema check above.
      while (iter.hasNext) {
        val row = iter.next()
        val ck = row.getLong(iCust).toInt
        val sk = row.getLong(iSupp).toInt
        val pk = row.getLong(iPart).toInt
        val dk = row.getLong(iDate).toInt

        // Bounds-checked O(1) probes, lower bound included: an out-of-range key
        // means no matching dimension row, so the fact row is dropped
        // (inner-join semantics) instead of throwing.
        if (ck >= 0 && ck < cn.length &&
            sk >= 0 && sk < so.length &&
            pk >= 0 && pk < po.length &&
            dk >= 0 && dk < dy.length &&
            so(sk) && po(pk)) {

          val nation = cn(ck)
          val year   = dy(dk)
          if (nation >= 0 && year >= 0) {
            val key  = year.toLong * NATION_RADIX + nation.toLong
            val prev = localAggs.getOrElse(key, 0.0)
            localAggs.update(key, prev + row.getDouble(iAmt))
          }
        }
      }

      localAggs.iterator.map { case (k, v) =>
        Row((k / NATION_RADIX).toInt, (k % NATION_RADIX).toInt, v)
      }
    }

    spark.createDataFrame(partialAggs, schema)
      .groupBy("d_year", "c_nation")
      .agg(sum("partial_agg").alias("total"))
  }

  // ---------------------------------------------------------------------------
  // Driver. `repartitionFact` defaults to None on purpose - see the header note.
  // ---------------------------------------------------------------------------
  def run(
      spark: SparkSession,
      dataDir: String,
      repartitionFact: Option[Int] = None
  ): DataFrame = {

    val cust = spark.read.parquet(s"$dataDir/customer")
    val supp = spark.read.parquet(s"$dataDir/supplier")
    val part = spark.read.parquet(s"$dataDir/part")
    val dat  = spark.read.parquet(s"$dataDir/date")

    val loRaw = spark.read.parquet(s"$dataDir/lineorder")
    val lo = repartitionFact.fold(loRaw) { n =>
      Console.err.println(
        s"[WARN] repartitioning the fact table to $n partitions adds a full " +
        s"exchange to the measured plan")
      loRaw.repartition(n)
    }

    val (custNationArr, nationDict) = buildIntArray(cust, "c_custkey", "c_nation")
    val (dateYearArr,   yearDict)   = buildIntArray(dat,  "d_datekey", "d_year")
    require(yearDict.isEmpty, "d_year must be numeric")

    if (nationDict.nonEmpty) {
      println("c_nation dictionary (value -> code):")
      nationDict.toSeq.sortBy(_._2).foreach { case (v, c) => println(f"  $c%4d  $v") }
    }

    val custNation = spark.sparkContext.broadcast(custNationArr)
    val dateYear   = spark.sparkContext.broadcast(dateYearArr)
    val suppOk     = spark.sparkContext.broadcast(buildFlagArray(supp, "s_suppkey"))
    val partOk     = spark.sparkContext.broadcast(buildFlagArray(part, "p_partkey"))

    joinlessMax(lo, custNation, suppOk, partOk, dateYear)
  }

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val cores   = if (args.length > 1) args(1) else "16"

    val spark = SparkSession.builder()
      .appName("JoinlessPartitionPattern")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")        // stable, reproducible plan
      .config("spark.sql.autoBroadcastJoinThreshold", "-1") // no implicit broadcast
      .getOrCreate()

    val t0      = System.nanoTime()
    val result  = run(spark, dataDir).cache()
    val groups  = result.count()
    val total   = result.agg(sum("total")).head().getDouble(0)
    val elapsed = (System.nanoTime() - t0) / 1e9

    println(f"groups        = $groups")
    println(f"total (check) = $total%.0f")
    println(f"elapsed       = $elapsed%.2f s")

    result.orderBy("d_year", "c_nation").show(200, truncate = false)
    spark.stop()
  }
}
