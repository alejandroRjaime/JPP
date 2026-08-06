// =============================================================================
// The scenario table of Case Study 3, reconstructed.
//
// PROVENANCE, because this matters for how the numbers may be used: this table
// is NOT the one the case study was measured over. That dataset does not exist
// in this repository or anywhere on this host. The schema and cardinality are
// reconstructed from the description in joinless_v5.tex:856-910 — three
// scenarios, ten million records, columns ENTITY_ID, INSTRUMENT_ID and a triple
// of VALUATION_METHOD_SC_i / PROTECTION_METHOD_SC_i / COUNTERPARTY_EXPOSURE_SCi.
//
// Note also that Case Study 3 was REMOVED from the manuscript: `.par` appears in
// versions v2 to v5 and in none of v8, v9 or v10. The measurements this table
// supports therefore concern material the paper no longer contains, and answer a
// reviewer comment written against an earlier version.
//
// Values are drawn with fixed seeds, per the rule established in
// reports/generador.md: the partition count is declared rather than inherited,
// because `rand` is seeded per partition and a different count produces a
// different table with the same schema and row count.
//
// Usage:  ./scripts/jpp GenerateScenarios <outDir> [rows] [partitions]
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object GenerateScenarios {

  final val ROWS       = 10000000L
  final val PARTITIONS = 64
  final val N_SCENARIOS = 3

  def main(args: Array[String]): Unit = {
    val outDir = if (args.nonEmpty)   args(0) else "/path/to/ssb_scenarios"
    val rows   = if (args.length > 1) args(1).toLong else ROWS
    val parts  = if (args.length > 2) args(2).toInt  else PARTITIONS
    val cores  = if (args.length > 3) args(3) else "16"

    val spark = SparkSession.builder()
      .appName("GenerateScenarios")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val valuations  = Array("MARK_TO_MARKET", "MARK_TO_MODEL", "HISTORICAL_COST", "FAIR_VALUE")
    val protections = Array("COLLATERAL", "NETTING", "GUARANTEE", "NONE")

    var df = spark.range(0, rows, 1, parts)
      .withColumn("ENTITY_ID",     concat(lit("ENT"), lpad(($"id" % 50000).cast("string"), 6, "0")))
      .withColumn("INSTRUMENT_ID", concat(lit("INS"), lpad($"id".cast("string"), 9, "0")))

    // One triple of columns per scenario, each with its own seed so that the
    // scenarios differ from one another rather than being copies.
    (1 to N_SCENARIOS).foreach { i =>
      df = df
        .withColumn(s"VALUATION_METHOD_SC_$i",
          element_at(lit(valuations), (rand(10 + i) * valuations.length).cast("int") + 1))
        .withColumn(s"PROTECTION_METHOD_SC_$i",
          element_at(lit(protections), (rand(20 + i) * protections.length).cast("int") + 1))
        .withColumn(s"COUNTERPARTY_EXPOSURE_SC$i",
          round(rand(30 + i) * 1000000, 2))
    }

    val cols = Seq("ENTITY_ID", "INSTRUMENT_ID") ++
      (1 to N_SCENARIOS).flatMap(i =>
        Seq(s"VALUATION_METHOD_SC_$i", s"PROTECTION_METHOD_SC_$i", s"COUNTERPARTY_EXPOSURE_SC$i"))

    df.select(cols.map(col): _*)
      .write.mode("overwrite").parquet(s"$outDir/scenarios")

    println(s"written to $outDir/scenarios  ($rows rows, $parts partitions, $N_SCENARIOS scenarios)")
    spark.stop()
  }
}
