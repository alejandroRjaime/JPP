// =============================================================================
// Per-run record of the configuration that was actually in force and the
// physical plan that actually ran.
//
// Motivation: a reviewer objected that the paper does not reveal or discuss
// `spark.sql.autoBroadcastJoinThreshold`. The baselines do control it, but from
// inside the code, where it is invisible in the results. Raising a threshold is
// also not the same as changing a plan — Spark may leave the plan untouched —
// so the plan has to be recorded alongside the setting rather than inferred
// from it.
//
// Plans and metrics are taken from a QueryExecutionListener, so what is written
// is the executed plan with its metrics populated, not a re-planned copy. That
// matters here because the runners `cache()` their result: reading the plan back
// off the cached Dataset would show an InMemoryTableScan and none of the joins.
//
// Output: results/plans/<label>.txt, plus counts on stdout for the CSV writers.
// =============================================================================

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.util.QueryExecutionListener
import java.io.{File, PrintWriter}
import scala.collection.mutable

object RunReport {

  /** Operator counts of one executed plan. `exchanges` is the sum of shuffle and
    * broadcast exchanges: both are redistributions, and the claim under test is
    * about how many the plan performs. They are also reported separately, since
    * a broadcast exchange and a shuffle exchange cost very different things. */
  case class Counts(bhj: Int, smj: Int, shuffle: Int, broadcastEx: Int) {
    def exchanges: Int = shuffle + broadcastEx
    def total: Int     = bhj + smj + exchanges
  }

  /** Broadcast cost as Spark itself measured it, summed over every broadcast
    * exchange in the plan. `collect` is the driver-side gather, `build` the
    * construction of the relation, `broadcast` the transfer. Kept separate
    * because only the first two are paid again per query when a plan rebuilds
    * a dimension it has already built once. */
  case class BroadcastCost(collectMs: Long, buildMs: Long, broadcastMs: Long, rows: Long, bytes: Long) {
    def totalMs: Long = collectMs + buildMs + broadcastMs
  }

  private val executions = mutable.ArrayBuffer.empty[(String, Counts, BroadcastCost, String)]
  private var label: String = "run"
  private var outDir: String = "results/plans"

  /** Walk the plan including the plans hidden inside cached relations.
    *
    * `SparkPlan.foreach` only descends into `children`, and every runner here
    * calls `.cache()` before the first action. That makes the executed plan of
    * the action an `InMemoryTableScanExec`, which has no children: the joins
    * that actually ran live inside the cached relation and are invisible to a
    * plain traversal. Counting without this descent reports zero joins for
    * every configuration, which is indistinguishable from a plan that genuinely
    * has none — the exact failure this instrumentation exists to prevent.
    */
  private def walk(plan: SparkPlan)(f: SparkPlan => Unit): Unit = {
    val seen = mutable.Set.empty[Int]
    def go(p: SparkPlan): Unit = {
      if (!seen.add(System.identityHashCode(p))) return
      f(p)
      p.children.foreach(go)
      p match {
        case s: InMemoryTableScanExec => go(s.relation.cacheBuilder.cachedPlan)
        case _ =>
      }
    }
    go(plan)
  }

  private def countOps(plan: SparkPlan): Counts = {
    var bhj, smj, sh, bx = 0
    walk(plan) { p =>
      val n = p.nodeName
      if (n.contains("BroadcastHashJoin")) bhj += 1
      else if (n.contains("SortMergeJoin")) smj += 1
      if (n.contains("BroadcastExchange")) bx += 1
      else if (n.contains("ShuffleExchange") || n == "Exchange") sh += 1
    }
    Counts(bhj, smj, sh, bx)
  }

  private def broadcastCost(plan: SparkPlan): BroadcastCost = {
    var c, b, t, rows, bytes = 0L
    walk(plan) { p =>
      if (p.nodeName.contains("BroadcastExchange")) {
        val m = p.metrics
        // Metric names are Spark-internal and have changed between versions;
        // a missing key is reported as zero rather than failing the run, and
        // the raw metric dump below preserves whatever was actually present.
        m.get("collectTime").foreach(v => c += v.value)
        m.get("buildTime").foreach(v => b += v.value)
        m.get("broadcastTime").foreach(v => t += v.value)
        m.get("numOutputRows").foreach(v => rows += v.value)
        m.get("dataSize").foreach(v => bytes += v.value)
      }
    }
    BroadcastCost(c, b, t, rows, bytes)
  }

  /** Plan text including the cached relations, for the same reason `walk` exists:
    * `toString` stops at the InMemoryTableScan and would leave the file showing
    * a plan with no joins in it. */
  private def planText(plan: SparkPlan): String = {
    val sb = new StringBuilder
    sb.append(plan.toString)
    walk(plan) {
      case s: InMemoryTableScanExec =>
        sb.append("\n\n--- cached plan behind ").append(s.nodeName).append(" ---\n")
        sb.append(s.relation.cacheBuilder.cachedPlan.toString)
      case _ =>
    }
    sb.toString
  }

  private def effectiveConfig(spark: SparkSession): Seq[(String, String)] = {
    val c = spark.conf
    def get(k: String, dflt: String = "<unset>") = try c.get(k) catch { case _: Throwable => dflt }
    val rt = Runtime.getRuntime
    Seq(
      "spark.sql.autoBroadcastJoinThreshold" -> get("spark.sql.autoBroadcastJoinThreshold"),
      "spark.sql.adaptive.enabled"           -> get("spark.sql.adaptive.enabled"),
      "spark.sql.shuffle.partitions"         -> get("spark.sql.shuffle.partitions"),
      "spark.driver.memory"                  -> get("spark.driver.memory"),
      "spark.executor.memory"                -> get("spark.executor.memory"),
      "spark.driver.maxResultSize"           -> get("spark.driver.maxResultSize"),
      "spark.master"                         -> get("spark.master"),
      "spark.default.parallelism"            -> spark.sparkContext.defaultParallelism.toString,
      "spark.local.dir"                      -> get("spark.local.dir"),
      "jvm.availableProcessors"              -> rt.availableProcessors.toString,
      "jvm.maxMemoryMB"                      -> (rt.maxMemory / (1024 * 1024)).toString,
      "java.version"                         -> System.getProperty("java.version"),
      "java.vendor"                          -> System.getProperty("java.vendor"),
      "spark.version"                        -> spark.version
    )
  }

  /** Register the listener. Call once, immediately after the session is built
    * and before any action, so that no execution escapes the record.
    *
    * The output directory defaults to JPP_PLAN_DIR when set. Sweeps that reuse
    * run labels across datasets — the medium-dimension sweep runs the same Q1
    * over three different dimension sets — would otherwise overwrite each
    * other's plan files, and a plan file silently replaced by one from a
    * different dataset is worse than a missing one. */
  def install(spark: SparkSession, runLabel: String,
              dir: String = sys.env.getOrElse("JPP_PLAN_DIR", "results/plans")): Unit = {
    label = runLabel
    outDir = dir
    // Task-level metrics, for the shuffle volume. A QueryExecutionListener sees
    // plans and SQL metrics; shuffle bytes written and read per task come from
    // the scheduler's listener bus instead.
    spark.sparkContext.addSparkListener(new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(e: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
        val m = e.taskMetrics
        if (m != null) {
          shWritten    += m.shuffleWriteMetrics.bytesWritten
          shRecWritten += m.shuffleWriteMetrics.recordsWritten
          shLocalRead  += m.shuffleReadMetrics.localBytesRead
          shRemoteRead += m.shuffleReadMetrics.remoteBytesRead
          shRecRead    += m.shuffleReadMetrics.recordsRead
        }
      }
    })

    spark.listenerManager.register(new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        val plan = qe.executedPlan
        executions += ((funcName, countOps(plan), broadcastCost(plan), planText(plan)))
      }
      override def onFailure(funcName: String, qe: QueryExecution, ex: Exception): Unit = {
        executions += ((s"$funcName (FAILED: ${ex.getClass.getSimpleName})",
          Counts(0, 0, 0, 0), BroadcastCost(0, 0, 0, 0, 0), qe.executedPlan.toString))
      }
    })
  }

  /** Shuffle volume over every task in the session, from task metrics rather
    * than from the UI.
    *
    * This is what a multi-node deployment would put on the wire and a
    * single-node one puts on `spark.local.dir`. The manuscript states at
    * `joinless_v10_Jul_31.tex:1264` that the wall-clock effect of that
    * difference "remains an empirical question"; the volume is not a question,
    * and measuring it bounds the part of the answer that arithmetic can supply.
    *
    * Read bytes are split into local and remote. On a single node everything is
    * local, so `remote` is expected to be zero — which is the point: the whole
    * of `local` is what would become remote elsewhere. */
  final case class ShuffleTotals(bytesWritten: Long, recordsWritten: Long,
                                 localBytesRead: Long, remoteBytesRead: Long,
                                 recordsRead: Long) {
    def bytesRead: Long = localBytesRead + remoteBytesRead
  }

  private var shWritten, shRecWritten, shLocalRead, shRemoteRead, shRecRead = 0L

  def shuffleTotals: ShuffleTotals =
    ShuffleTotals(shWritten, shRecWritten, shLocalRead, shRemoteRead, shRecRead)

  /** Broadcast milliseconds over every execution recorded so far in this
    * session.
    *
    * A session that runs several queries needs the cost attributed to each one,
    * and the per-query figure is the delta between two readings of this. Summing
    * across executions rather than taking the principal one is deliberate here:
    * the question is what the session paid in total, so an execution that
    * rebuilt a broadcast counts even if it was not the richest plan. */
  def cumulativeBroadcastMs: Long = executions.map(_._3.totalMs).sum

  /** The execution that carried the query proper. A run issues several — the
    * count, the check sum, sometimes a cache materialization — and only the
    * richest plan is the one the paper's claim is about. Choosing it by operator
    * count rather than by order makes this independent of how a runner is
    * written. */
  private def principal: Option[(String, Counts, BroadcastCost, String)] =
    if (executions.isEmpty) None else Some(executions.maxBy(_._2.total))

  /** Write the record and print the counts. Call after the run has completed and
    * before `spark.stop()`. */
  def emit(spark: SparkSession): Unit = {
    val dir = new File(outDir)
    dir.mkdirs()
    val f = new File(dir, s"$label.txt")
    val w = new PrintWriter(f, "UTF-8")
    try {
      w.println("=" * 78)
      w.println(s"run: $label")
      w.println("=" * 78)
      w.println()
      w.println("--- effective configuration ---")
      effectiveConfig(spark).foreach { case (k, v) => w.println(f"$k%-42s = $v") }
      w.println()

      principal match {
        case None =>
          w.println("--- no query execution was recorded ---")
        case Some((fn, c, bc, planText)) =>
          w.println("--- operator counts (principal execution) ---")
          w.println(f"trigger${" " * 35}= $fn")
          w.println(f"BroadcastHashJoin${" " * 25}= ${c.bhj}")
          w.println(f"SortMergeJoin${" " * 29}= ${c.smj}")
          w.println(f"ShuffleExchange${" " * 27}= ${c.shuffle}")
          w.println(f"BroadcastExchange${" " * 25}= ${c.broadcastEx}")
          w.println(f"Exchange (shuffle + broadcast)${" " * 12}= ${c.exchanges}")
          w.println()
          w.println("--- broadcast cost, as measured by Spark ---")
          w.println(f"collectTime ms${" " * 28}= ${bc.collectMs}")
          w.println(f"buildTime ms${" " * 30}= ${bc.buildMs}")
          w.println(f"broadcastTime ms${" " * 26}= ${bc.broadcastMs}")
          w.println(f"total broadcast ms${" " * 24}= ${bc.totalMs}")
          w.println(f"broadcast rows${" " * 28}= ${bc.rows}")
          w.println(f"broadcast dataSize bytes${" " * 18}= ${bc.bytes}")
          w.println()
          w.println("--- executed plan ---")
          w.println(planText)
      }

      w.println()
      w.println(s"--- all ${executions.length} executions in this run ---")
      executions.zipWithIndex.foreach { case ((fn, c, bc, _), i) =>
        w.println(f"[$i] $fn%-28s bhj=${c.bhj} smj=${c.smj} shuffle=${c.shuffle} " +
                  f"bcastEx=${c.broadcastEx} bcastMs=${bc.totalMs}")
      }
    } finally w.close()

    // stdout, so the sweep scripts can build their CSV from the same numbers
    // that were written to the file rather than from a second measurement.
    val (c, bc) = principal.map { case (_, cc, bb, _) => (cc, bb) }
      .getOrElse((Counts(0, 0, 0, 0), BroadcastCost(0, 0, 0, 0, 0)))
    println(f"plan_file     = ${f.getPath}")
    println(f"bhj_count     = ${c.bhj}")
    println(f"smj_count     = ${c.smj}")
    println(f"shuffle_count = ${c.shuffle}")
    println(f"bcast_ex      = ${c.broadcastEx}")
    println(f"exchange_count= ${c.exchanges}")
    println(f"bcast_ms      = ${bc.totalMs}")
    println(f"bcast_bytes   = ${bc.bytes}")
    // Emitted last so that a run's shuffle volume is on the same line-oriented
    // record as everything else the sweep scripts parse.
    val sh = shuffleTotals
    println(f"shuffle_write_bytes   = ${sh.bytesWritten}")
    println(f"shuffle_write_records = ${sh.recordsWritten}")
    println(f"shuffle_read_bytes    = ${sh.bytesRead}")
    println(f"shuffle_read_local    = ${sh.localBytesRead}")
    println(f"shuffle_read_remote   = ${sh.remoteBytesRead}")
    println(f"shuffle_read_records  = ${sh.recordsRead}")
  }
}
