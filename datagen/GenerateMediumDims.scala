// =============================================================================
// Dimensions in the 10 MB - 1 GB range the paper motivates the pattern over.
//
// The dimensions of the main dataset are far below that range — customer is
// about 12 MB on disk — so the regime the paper argues for is never exercised.
// This generator produces the same schema at larger scales, so the threshold
// sweep can be repeated where the argument actually applies.
//
// Design constraint that makes the result comparable: the fact table is NOT
// regenerated. The additional dimension rows are appended above the key range
// the fact table references, so every fact row still finds exactly one match
// and the group count and aggregate sum are unchanged. Growing the dimension
// downward instead — remapping the keys the fact table uses — would change the
// answer and the point of comparison with it.
//
// The consequence is deliberate and must be stated when the numbers are used:
// the added rows are never probed. A relational plan still builds and broadcasts
// a hash table over all of them, and the pattern still allocates an array sized
// by the largest key, so both pay for the size of the dimension while neither
// gains selectivity from it. That is the honest analogue of a wide dimension
// whose rows are mostly irrelevant to a given query, and it is the case the
// reviewer's question is about.
//
// Usage:  ./scripts/jpp GenerateMediumDims <outDir> <targetMB>[,<targetMB>...]
//   e.g.  ./scripts/jpp GenerateMediumDims ~/ssb_medium 10,100,1000
//
// Each target produces <outDir>/dim<targetMB>mb/{customer,supplier,part,date}
// with the fact table symlinked or referenced separately by the sweep.
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io.File
import java.nio.file.{Files, Paths}

object GenerateMediumDims {

  /** Key range the fact table of the main dataset references. Dimension rows at
    * or above this key are additions that no fact row probes. */
  final val FACT_CUSTKEYS = 3000000L

  /** Dimension partitioning, fixed rather than left to the environment.
    *
    * These dimension columns are deterministic functions of the row id, so the
    * partition count does not change their values the way it changes a `rand`
    * column. It does change the file count, and therefore the read parallelism
    * of every measurement taken over them, which is enough to make two runs
    * incomparable. The value is the one the already-published measurements were
    * taken at, so regenerating reproduces them rather than silently shifting
    * them. */
  final val DIM_PARTITIONS = 16


  private def dirBytes(path: String): Long = {
    val f = new File(path)
    if (!f.exists) 0L
    else if (f.isFile) f.length
    else Option(f.listFiles()).map(_.map(x => dirBytes(x.getPath)).sum).getOrElse(0L)
  }

  def main(args: Array[String]): Unit = {
    val outDir  = if (args.nonEmpty) args(0) else "/path/to/ssb_medium"
    val targets = if (args.length > 1) args(1).split(",").map(_.trim.toInt).toSeq
                  else Seq(10, 100, 1000)
    // Path to the existing fact table, linked into each target directory so the
    // runners can be pointed at one directory without any change to them.
    val factDir = if (args.length > 2) Some(args(2)) else None
    val cores   = if (args.length > 3) args(3) else "16"

    val spark = SparkSession.builder()
      .appName("GenerateMediumDims")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val regions = Array("AMERICA", "ASIA", "EUROPE", "AFRICA", "MIDDLE EAST")
    val mfgrs   = Array("MFGR#1", "MFGR#2", "MFGR#3", "MFGR#4", "MFGR#5")
    val nations = Array(
      "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "CHINA",
      "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA",
      "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN",
      "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "ROMANIA",
      "RUSSIA", "SAUDI ARABIA", "UNITED KINGDOM", "UNITED STATES", "VIETNAM")

    // Bytes per customer row on disk, measured once from the existing dimension
    // rather than assumed: Parquet dictionary-encodes these columns heavily, so
    // an estimate from the logical row width would be wrong by an order of
    // magnitude and every target size would miss.
    val probeRows = 3000000L
    val probeDir  = s"$outDir/_probe"
    spark.range(0, probeRows)
      .withColumn("c_custkey", $"id")
      .withColumn("c_nation", element_at(lit(nations), (($"id" % 25) + 1).cast("int")))
      .withColumn("c_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
      .select("c_custkey", "c_nation", "c_region")
      .write.mode("overwrite").parquet(probeDir)
    val bytesPerRow = dirBytes(probeDir).toDouble / probeRows
    println(f"measured $bytesPerRow%.4f bytes/row on disk for the customer schema")

    targets.foreach { mb =>
      val targetBytes = mb.toLong * 1024L * 1024L
      val nCust = math.max(FACT_CUSTKEYS, (targetBytes / bytesPerRow).toLong)
      val dir   = s"$outDir/dim${mb}mb"

      // Keys stay dense in 0..nCust-1, which the indexed-array lookup requires.
      // Rows below FACT_CUSTKEYS keep the attribute assignment of the main
      // dataset, so the queries return the same answer.
      spark.range(0, nCust, 1, DIM_PARTITIONS)
        .withColumn("c_custkey", $"id")
        .withColumn("c_nation", element_at(lit(nations), (($"id" % 25) + 1).cast("int")))
        .withColumn("c_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
        .select("c_custkey", "c_nation", "c_region")
        .write.mode("overwrite").parquet(s"$dir/customer")

      // The other three dimensions are copied at their original scale: the
      // experiment varies one dimension at a time, so that a change in the plan
      // can be attributed to the size of the dimension that changed.
      spark.range(0, 200000L, 1, DIM_PARTITIONS)
        .withColumn("s_suppkey", $"id")
        .withColumn("s_region", element_at(lit(regions), (($"id" % 5) + 1).cast("int")))
        .select("s_suppkey", "s_region")
        .write.mode("overwrite").parquet(s"$dir/supplier")

      spark.range(0, 1400000L, 1, DIM_PARTITIONS)
        .withColumn("p_partkey", $"id")
        .withColumn("p_mfgr", element_at(lit(mfgrs), (($"id" % 5) + 1).cast("int")))
        .select("p_partkey", "p_mfgr")
        .write.mode("overwrite").parquet(s"$dir/part")

      spark.range(0, 2556L, 1, DIM_PARTITIONS)
        .withColumn("d_datekey", $"id")
        .withColumn("d_year", (lit(1992) + ($"id" / 365).cast("int")).cast("string"))
        .select("d_datekey", "d_year")
        .write.mode("overwrite").parquet(s"$dir/date")

      // The fact table is shared by reference, not copied: 24 GB per target
      // would be both wasteful and a second variable, since a fresh copy would
      // have a different physical layout on disk from the one every other
      // measurement in the paper was taken against.
      factDir.foreach { src =>
        val link = Paths.get(s"$dir/lineorder")
        if (!Files.exists(link)) Files.createSymbolicLink(link, Paths.get(src).toAbsolutePath)
      }

      val actual = dirBytes(s"$dir/customer")
      println(f"target ${mb}%5d MB -> customer rows=$nCust%,12d  actual=$actual%,14d bytes " +
              f"(${actual / 1048576.0}%8.2f MiB)  keys probed by fact: $FACT_CUSTKEYS%,d")
    }

    println(s"\nwritten to $outDir")
    println("The fact table is not regenerated; point the sweep at the original lineorder.")
    spark.stop()
  }
}
