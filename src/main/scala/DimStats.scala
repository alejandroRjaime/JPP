// =============================================================================
// Sizes of the dimensions, measured three ways, because the three differ and
// each answers a different question:
//
//   on-disk bytes      what the Parquet files occupy.
//   optimizer estimate what `spark.sql.autoBroadcastJoinThreshold` is actually
//                      compared against. This is the only one that determines
//                      whether Spark broadcasts, so a threshold sweep has to be
//                      derived from it rather than from the file sizes.
//   lookup bytes       what the pattern's broadcast structure occupies in the
//                      driver and in every executor, measured with Spark's own
//                      SizeEstimator rather than computed from element counts.
//
// The last one answers a reviewer's question directly: whether the in-memory
// lookup structure can end up larger than the raw data it replaces. It can —
// the array is indexed by surrogate key, so it is sized by the largest key, not
// by the number of rows, and a sparse key space inflates it without bound.
//
// Usage:  ./scripts/jpp DimStats <dataDir> [csvOut]
// =============================================================================

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.util.SizeEstimator
import java.io.{File, PrintWriter}

object DimStats {

  case class DimSpec(name: String, keyCol: String, projection: Seq[String], valueCol: Option[String])

  /** Projections match what the baselines select, so the estimate reported here
    * is the one the optimizer forms for the plan actually measured. */
  val DIMS = Seq(
    DimSpec("customer", "c_custkey", Seq("c_custkey", "c_nation"), Some("c_nation")),
    DimSpec("supplier", "s_suppkey", Seq("s_suppkey"),             None),
    DimSpec("part",     "p_partkey", Seq("p_partkey"),             None),
    DimSpec("date",     "d_datekey", Seq("d_datekey", "d_year"),   Some("d_year"))
  )

  private def dirBytes(path: String): Long = {
    val f = new File(path)
    if (!f.exists) -1L
    else if (f.isFile) f.length
    else Option(f.listFiles()).map(_.map(x => dirBytes(x.getPath)).sum).getOrElse(0L)
  }

  /** What the optimizer estimates for a dimension once it is cached.
    *
    * Caching replaces the Parquet scan with an `InMemoryRelation`, whose size
    * estimate is the in-memory representation rather than the compressed bytes
    * on disk. That estimate is what `autoBroadcastJoinThreshold` is compared
    * against, so caching can push a dimension past a threshold it previously sat
    * under and silently convert a broadcast join into a sort-merge join. This
    * prints both estimates so the effect can be quantified instead of inferred
    * from a slowdown.
    */
  private def cachedEstimates(spark: SparkSession, dataDir: String): Unit = {
    println("dimension    projected(disk)      projected(cached)     ratio  >10MiB?")
    DIMS.foreach { d =>
      val path = s"$dataDir/${d.name}"
      if (new File(path).exists) {
        val raw  = spark.read.parquet(path)
        val cold = raw.select(d.projection.map(col): _*)
          .queryExecution.optimizedPlan.stats.sizeInBytes
        val cached = spark.read.parquet(path)
        cached.cache()
        cached.count()
        val warm = cached.select(d.projection.map(col): _*)
          .queryExecution.optimizedPlan.stats.sizeInBytes
        val over = if (warm.toLong > 10485760L) "YES" else "no"
        println(f"${d.name}%-10s ${cold}%,16d  ${warm}%,20d  " +
                f"${warm.toDouble / cold.toDouble}%6.2fx  $over")
        cached.unpersist()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.nonEmpty)   args(0) else "/path/to/ssb_synth"
    val csvOut  = if (args.length > 1) args(1) else "results/dimension_sizes.csv"
    val cores   = if (args.length > 2) args(2) else "16"

    val spark = SparkSession.builder()
      .appName("DimStats")
      .master(sys.env.getOrElse("SPARK_MASTER", s"local[$cores]"))
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    // `--cached-estimates` answers a single question and exits: how much caching
    // inflates what the broadcast threshold is compared against.
    if (args.contains("--cached-estimates")) {
      cachedEstimates(spark, dataDir)
      spark.stop()
      return
    }

    val rows = DIMS.flatMap { d =>
      val path = s"$dataDir/${d.name}"
      if (!new File(path).exists) {
        Console.err.println(s"[SKIP] $path does not exist")
        None
      } else {
        val df   = spark.read.parquet(path)
        val proj = df.select(d.projection.map(col): _*)

        val nRows    = df.count()
        val onDisk   = dirBytes(path)
        val estFull  = df.queryExecution.optimizedPlan.stats.sizeInBytes
        val estProj  = proj.queryExecution.optimizedPlan.stats.sizeInBytes

        // The largest surrogate key, obtained with an aggregate rather than by
        // collecting the dimension. This is what sizes the lookup array, and it
        // is computed first so that the predicted size is available even when
        // actually building the array fails — which is the interesting case.
        val maxKey = df.agg(max(col(d.keyCol))).head().get(0) match {
          case v: java.lang.Long    => v.longValue()
          case v: java.lang.Integer => v.longValue()
          case null                 => -1L
          case other                => other.toString.toLong
        }
        val elems = maxKey + 1
        // 4 bytes per Int slot, 1 per Boolean, plus the array object header.
        val predictedBytes = (if (d.valueCol.isDefined) 4L * elems else elems) + 16L

        // The lookup structure the pattern would actually build: an Int array
        // when the dimension contributes an attribute, a Boolean flag array when
        // it only enforces the join.
        //
        // Phase 1 collects the whole dimension to the driver, so at large
        // dimension sizes this is where the pattern breaks — through
        // spark.driver.maxResultSize or through driver memory. That is a real
        // limit of the pattern and a result worth reporting, so it is caught and
        // recorded rather than allowed to abort the measurement of every other
        // dimension.
        val (lookupBytes, lookupStatus) =
          try {
            val arr: AnyRef = d.valueCol match {
              case Some(v) => JoinlessPartitionPattern.buildIntArrayAuto(df, d.keyCol, v)._1
              case None    => JoinlessPartitionPattern.buildFlagArrayAuto(df, d.keyCol)
            }
            (SizeEstimator.estimate(arr), "measured")
          } catch {
            case t: Throwable =>
              val kind =
                if (Option(t.getMessage).exists(_.contains("maxResultSize"))) "fail:maxResultSize"
                else if (t.isInstanceOf[OutOfMemoryError]) "fail:OOM"
                else s"fail:${t.getClass.getSimpleName}"
              Console.err.println(s"[${d.name}] lookup build failed: $kind — ${t.getMessage}")
              (-1L, kind)
          }
        val lookupElems = elems

        val shown = if (lookupBytes >= 0) lookupBytes else predictedBytes
        println(f"${d.name}%-10s rows=$nRows%,12d  disk=$onDisk%,14d  " +
                f"estFull=$estFull%,14d  estProj=$estProj%,14d  " +
                f"lookup=$shown%,14d ($lookupStatus)  " +
                f"lookup/disk=${shown.toDouble / onDisk}%6.3f")

        Some((d.name, nRows, onDisk, estFull.toLong, estProj.toLong,
              lookupBytes, predictedBytes, lookupStatus, lookupElems, maxKey))
      }
    }

    new File(csvOut).getAbsoluteFile.getParentFile.mkdirs()
    val w = new PrintWriter(csvOut, "UTF-8")
    try {
      // `lookup_bytes` is -1 when the structure could not be built; the ratios
      // then use `lookup_bytes_predicted`, and `lookup_status` says which of the
      // two the row carries. Reporting a predicted size as if it had been
      // measured would hide exactly the failure this column exists to record.
      w.println("dimension,rows,disk_bytes,est_full_bytes,est_projected_bytes," +
                "lookup_bytes,lookup_bytes_predicted,lookup_status,lookup_elements,max_key," +
                "lookup_over_disk,lookup_over_est_projected")
      rows.foreach { case (n, r, disk, ef, ep, lb, pb, st, le, mk) =>
        val eff = if (lb >= 0) lb else pb
        // Locale.ROOT, not the default locale: this host formats decimals with a
        // comma, which splits the field in a comma-separated file and silently
        // shifts every column after it.
        def d6(x: Double) = String.format(java.util.Locale.ROOT, "%.6f", Double.box(x))
        w.println(s"$n,$r,$disk,$ef,$ep,$lb,$pb,$st,$le,$mk," +
                  s"${d6(eff.toDouble / disk)},${d6(eff.toDouble / ep)}")
      }
    } finally w.close()

    println(s"\nwrote $csvOut")

    // Thresholds for the sweep, derived from the estimates rather than chosen as
    // round numbers: one just above each dimension's projected estimate, so that
    // each successive value admits exactly one more dimension to a broadcast.
    val est = rows.map { case (n, _, _, _, ep, _, _, _, _, _) => (n, ep) }.sortBy(_._2)
    println("\n--- projected estimates, ascending (the quantity the threshold is compared against) ---")
    est.foreach { case (n, b) => println(f"  $n%-10s $b%,14d bytes  (${b / 1048576.0}%8.3f MiB)") }
    println("\n--- sweep points admitting 1..N dimensions ---")
    est.zipWithIndex.foreach { case ((n, b), i) =>
      println(f"  admits ${i + 1} dim(s) (through $n%-10s): ${b + 1}%,14d bytes")
    }

    spark.stop()
  }
}
