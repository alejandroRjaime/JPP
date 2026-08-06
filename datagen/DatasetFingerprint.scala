// =============================================================================
// Cheap, exact check that a dataset is the one the published numbers came from.
//
// Run before a sweep. It catches in one pass what would otherwise be discovered
// hours later, or never: a fact table regenerated at a different partition
// count, a dimension rebuilt with a different rule, a directory pointing
// somewhere unintended.
//
// The fingerprint is the sum of the IEEE-754 bit patterns of `amount`. It is
// exact and independent of summation order, so it agrees only if every one of
// the 600,000,000 values is identical bit for bit. The ordinary double sum
// cannot be used for this: it varies with the reduction tree, which varies with
// the physical layout, so it would report a difference between two identical
// tables read through different partitionings. That mistake was made once
// already — see reports/generador.md §4.
//
// Row count and per-key-column sums are checked alongside it, so that a dataset
// which differs in the keys but happens to match on `amount` is still caught.
//
// Usage:
//   ./scripts/jpp DatasetFingerprint <dataDir>            # check against the published values
//   ./scripts/jpp DatasetFingerprint <dataDir> --emit     # print values for a new dataset
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DatasetFingerprint {

  /** The published dataset, from reports/generador.md §4. */
  final val EXPECTED_ROWS          = 600000000L
  final val EXPECTED_AMOUNT_BITS   = BigDecimal("2796171100739202574311426188")
  final val EXPECTED_SUM_ORDERKEY  = 179999999700000000L
  final val EXPECTED_SUM_CUSTKEY   = 900022254009281L
  final val EXPECTED_SUM_SUPPKEY   = 60001169805653L
  final val EXPECTED_SUM_PARTKEY   = 420010238860186L
  final val EXPECTED_SUM_ORDERDATE = 766518981463L

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.nonEmpty) args(0) else "/home/athenas/ssb_synth"
    val emit    = args.contains("--emit")
    val cores   = sys.env.getOrElse("JPP_FINGERPRINT_CORES", "16")

    val spark = SparkSession.builder()
      .appName("DatasetFingerprint")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    val lo = spark.read.parquet(s"$dataDir/lineorder")

    val bits = udf((d: java.lang.Double) =>
      if (d == null) 0L else java.lang.Double.doubleToRawLongBits(d.doubleValue()))

    val row = lo.agg(
      count(lit(1)).alias("n"),
      sum("lo_orderkey").alias("sum_orderkey"),
      sum("lo_custkey").alias("sum_custkey"),
      sum("lo_suppkey").alias("sum_suppkey"),
      sum("lo_partkey").alias("sum_partkey"),
      sum("lo_orderdate").alias("sum_orderdate"),
      sum(bits(col("amount")).cast("decimal(38,0)")).alias("sum_amount_bits")
    ).head()

    val n         = row.getAs[Long]("n")
    val orderkey  = row.getAs[Long]("sum_orderkey")
    val custkey   = row.getAs[Long]("sum_custkey")
    val suppkey   = row.getAs[Long]("sum_suppkey")
    val partkey   = row.getAs[Long]("sum_partkey")
    val orderdate = row.getAs[Long]("sum_orderdate")
    val amountBits = BigDecimal(row.getAs[java.math.BigDecimal]("sum_amount_bits"))

    println(s"dataset       = $dataDir")
    println(f"partitions    = ${lo.rdd.getNumPartitions}")
    println(f"rows          = $n")
    println(s"sum_orderkey  = $orderkey")
    println(s"sum_custkey   = $custkey")
    println(s"sum_suppkey   = $suppkey")
    println(s"sum_partkey   = $partkey")
    println(s"sum_orderdate = $orderdate")
    println(s"sum_amount_bits = ${amountBits.toBigInt}")

    if (emit) {
      println("\n--emit given: values printed, nothing checked")
      spark.stop()
      return
    }

    val checks = Seq(
      ("rows",            n.toString,                  EXPECTED_ROWS.toString),
      ("sum_orderkey",    orderkey.toString,           EXPECTED_SUM_ORDERKEY.toString),
      ("sum_custkey",     custkey.toString,            EXPECTED_SUM_CUSTKEY.toString),
      ("sum_suppkey",     suppkey.toString,            EXPECTED_SUM_SUPPKEY.toString),
      ("sum_partkey",     partkey.toString,            EXPECTED_SUM_PARTKEY.toString),
      ("sum_orderdate",   orderdate.toString,          EXPECTED_SUM_ORDERDATE.toString),
      ("sum_amount_bits", amountBits.toBigInt.toString, EXPECTED_AMOUNT_BITS.toBigInt.toString))

    val bad = checks.filterNot { case (_, got, want) => got == want }

    println()
    if (bad.isEmpty) {
      println("FINGERPRINT OK — this is the dataset the published numbers were taken over")
    } else {
      bad.foreach { case (name, got, want) =>
        println(s"  $name: got $got, expected $want")
      }
      spark.stop()
      throw new IllegalStateException(
        s"dataset fingerprint does not match at $dataDir (${bad.size} of ${checks.size} " +
        "checks failed). Any measurement taken over it is not comparable with the " +
        "published tables.")
    }

    spark.stop()
  }
}
