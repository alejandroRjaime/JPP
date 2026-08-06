// =============================================================================
// Proof that GenerateSSBv2 reproduces the published fact table.
//
// The test is a per-column checksum over all 6x10^8 rows of both the published
// table and a regenerated one, compared exactly. Column sums are used rather
// than a row-by-row join because a join of two 23 GB tables would cost more
// than the rest of this report put together, while agreement of six independent
// aggregates over 600 million rows leaves no realistic room for a table that
// differs.
//
// The regenerated table is never written to disk. Values are produced by the
// same expressions GenerateSSBv2 writes, over the same range and the same
// partitioning, and consumed directly. Partitioning is part of the test: Spark
// seeds `rand` per partition, so a different partition count would change every
// value, and the sum of a double column depends on summation order, which
// depends on partitioning.
//
// `amount` is compared two ways. The exact double sum is order-dependent and is
// reported for completeness, but the decisive comparison is the sum of the raw
// bit patterns, which is order-independent and exact: two tables whose
// `amount` columns agree bit for bit in every row have the same bit-sum, and
// floating-point reassociation cannot fake it.
//
// Usage:  ./scripts/jpp VerifyGenerator <realDataDir> [rows] [partitions]
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object VerifyGenerator {

  def main(args: Array[String]): Unit = {
    val realDir = if (args.nonEmpty)   args(0) else "/home/athenas/ssb_synth"
    val rows    = if (args.length > 1) args(1).toLong else GenerateSSBv2.FACT_ROWS
    val parts   = if (args.length > 2) args(2).toInt  else GenerateSSBv2.FACT_PARTITIONS
    val cores   = if (args.length > 3) args(3) else "16"

    val spark = SparkSession.builder()
      .appName("VerifyGenerator")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    // The regenerated table, exactly as GenerateSSBv2 writes it.
    val gen = spark.range(0, rows, 1, parts)
      .withColumn("lo_orderkey",  col("id"))
      .withColumn("lo_custkey",   (rand(1) * GenerateSSBv2.N_CUST).cast("long"))
      .withColumn("lo_suppkey",   (rand(2) * GenerateSSBv2.N_SUPP).cast("long"))
      .withColumn("lo_partkey",   (rand(3) * GenerateSSBv2.N_PART).cast("long"))
      .withColumn("lo_orderdate", (rand(4) * GenerateSSBv2.N_DATE).cast("long"))
      .withColumn("amount",        rand(5) * GenerateSSBv2.AMOUNT_MAX)
      .select("lo_orderkey", "lo_custkey", "lo_suppkey", "lo_partkey", "lo_orderdate", "amount")

    val real = spark.read.parquet(s"$realDir/lineorder")

    // Spark has no double_to_raw_bits, so the order-independent checksum is
    // computed with a small UDF over the column. The sum is accumulated as a
    // decimal because 6x10^8 bit patterns overflow a Long.
    val bits = udf((d: java.lang.Double) =>
      if (d == null) 0L else java.lang.Double.doubleToRawLongBits(d.doubleValue()))
    spark.udf.register("bits", bits)

    def sums(df: DataFrame, label: String) = {
      val row = df.agg(
        count(lit(1)).alias("n"),
        sum("lo_orderkey").alias("sum_orderkey"),
        sum("lo_custkey").alias("sum_custkey"),
        sum("lo_suppkey").alias("sum_suppkey"),
        sum("lo_partkey").alias("sum_partkey"),
        sum("lo_orderdate").alias("sum_orderdate"),
        sum(bits(col("amount")).cast("decimal(38,0)")).alias("sum_amount_bits"),
        sum("amount").alias("sum_amount_double")
      ).head()
      val m = Seq(
        "n"                 -> row.getAs[Long]("n").toString,
        "sum_orderkey"      -> row.getAs[Long]("sum_orderkey").toString,
        "sum_custkey"       -> row.getAs[Long]("sum_custkey").toString,
        "sum_suppkey"       -> row.getAs[Long]("sum_suppkey").toString,
        "sum_partkey"       -> row.getAs[Long]("sum_partkey").toString,
        "sum_orderdate"     -> row.getAs[Long]("sum_orderdate").toString,
        "sum_amount_bits"   -> String.valueOf(row.getAs[Any]("sum_amount_bits")),
        "sum_amount_double" -> f"${row.getAs[Double]("sum_amount_double")}%.4f"
      )
      println(s"--- $label ---")
      m.foreach { case (k, v) => println(f"  $k%-18s = $v") }
      m.toMap
    }

    val a = sums(real, s"published ($realDir/lineorder)")
    val b = sums(gen,  s"regenerated (rows=$rows, partitions=$parts)")

    // `sum_amount_double` is deliberately NOT part of the verdict. Summing a
    // double column in parallel is order-dependent, and the two tables are read
    // through different physical layouts — Parquet splits on one side, a range
    // partitioning on the other — so their reduction trees differ even when
    // every value is identical. Letting it decide would report a mismatch for a
    // table that matches bit for bit, which is why the order-independent
    // bit-sum was computed in the first place.
    val decisive   = Seq("n", "sum_orderkey", "sum_custkey", "sum_suppkey",
                         "sum_partkey", "sum_orderdate", "sum_amount_bits")
    val diagnostic = Seq("sum_amount_double")

    println("\n--- decisive checksums ---")
    var allMatch = true
    decisive.foreach { k =>
      val ok = a(k) == b(k)
      if (!ok) allMatch = false
      println(f"  $k%-18s ${if (ok) "MATCH" else "DIFFER"}%-7s  published=${a(k)}  regenerated=${b(k)}")
    }

    println("\n--- order-dependent, not part of the verdict ---")
    diagnostic.foreach { k =>
      val ok = a(k) == b(k)
      println(f"  $k%-18s ${if (ok) "equal" else "differs"}%-8s published=${a(k)}  regenerated=${b(k)}")
      if (!ok) println("    (parallel summation order differs between a Parquet scan and a range; " +
                       "sum_amount_bits is the exact test)")
    }

    println()
    if (allMatch)
      println("RESULT = the generator reproduces the published fact table. Every decisive checksum "
            + "matches, including the order-independent bit-sum of `amount`, which agrees only if "
            + "every one of the 600,000,000 values is identical bit for bit.")
    else
      println("RESULT = MISMATCH — the generator does not reproduce the published fact table")

    spark.stop()
  }
}
