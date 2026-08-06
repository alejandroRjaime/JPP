// =============================================================================
// Partitioning is part of the experiment, and this makes it fail loudly when it
// is not what was declared.
//
// Two distinct hazards, both of which have already cost this work real damage:
//
//   Generation. `rand` is seeded per partition, so `spark.range(0, 6e8, 1, 2000)`
//   with any other partition count produces a DIFFERENT TABLE with the same
//   schema and the same row count. A dataset regenerated at the environment's
//   default parallelism looks correct and is not.
//
//   Execution. The manuscript reports 250 Spark partitions on read over the 2000
//   files. If two configurations being compared read with different parallelism,
//   the difference in time is not a property of the pattern and the table means
//   nothing.
//
// A warning is not enough for either: a warning in a log that nobody reads at
// the time is how a wrong number reaches a table. The run is invalidated.
//
// Declared values arrive through JPP_EXPECT_PARTITIONS as a comma-separated list
// of name=count, e.g.
//
//   JPP_EXPECT_PARTITIONS="lineorder=250,customer=1,supplier=1,part=1,date=1"
//
// A name that is absent from the list is reported but not enforced, so a caller
// can pin only what matters to it. With the variable unset nothing is enforced
// and the counts are still printed — the counts are always emitted, because a
// number that was never recorded cannot be checked afterwards.
// =============================================================================

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File

object PartitionGuard {

  final case class Observed(name: String, partitions: Int, expected: Option[Int]) {
    def ok: Boolean = expected.forall(_ == partitions)
  }

  private def expectations(): Map[String, Int] =
    sys.env.get("JPP_EXPECT_PARTITIONS").filter(_.trim.nonEmpty).map { spec =>
      spec.split(",").toSeq.flatMap { kv =>
        kv.split("=") match {
          case Array(k, v) => Some(k.trim -> v.trim.toInt)
          case _ =>
            throw new IllegalArgumentException(
              s"malformed JPP_EXPECT_PARTITIONS entry '$kv'; expected name=count")
        }
      }.toMap
    }.getOrElse(Map.empty)

  /** Observe and enforce. `tables` maps a logical name to the DataFrame whose
    * partitioning is being pinned.
    *
    * `rdd.getNumPartitions` is used rather than an estimate: it is the number of
    * tasks the scan will actually run, which is the quantity that has to match.
    * It does not read the data. */
  def check(tables: Seq[(String, DataFrame)], context: String = "run"): Seq[Observed] = {
    val exp = expectations()
    val observed = tables.map { case (name, df) =>
      Observed(name, df.rdd.getNumPartitions, exp.get(name))
    }

    observed.foreach { o =>
      val suffix = o.expected match {
        case Some(e) if e == o.partitions => s"  (expected $e, OK)"
        case Some(e)                      => s"  (expected $e, MISMATCH)"
        case None                         => "  (not pinned)"
      }
      println(f"partitions.${o.name}%-16s = ${o.partitions}%6d$suffix")
    }

    val bad = observed.filterNot(_.ok)
    if (bad.nonEmpty) {
      val detail = bad.map(o => s"${o.name}: got ${o.partitions}, expected ${o.expected.get}")
        .mkString("; ")
      throw new IllegalStateException(
        s"[$context] partitioning does not match what was declared: $detail. " +
        "The run is invalidated rather than reported: a comparison between " +
        "configurations that read with different parallelism measures the " +
        "parallelism, not the pattern.")
    }
    observed
  }

  /** Convenience for the standard star schema: pins the fact table and the four
    * dimensions under their usual names. */
  def checkStandard(spark: SparkSession, dataDir: String,
                    factName: String = "lineorder", context: String = "run"): Seq[Observed] = {
    val names = Seq(factName, "customer", "supplier", "part", "date")
    val tables = names.flatMap { n =>
      val p = s"$dataDir/$n"
      if (new File(p).exists) Some(n -> spark.read.parquet(p)) else None
    }
    check(tables, context)
  }
}
