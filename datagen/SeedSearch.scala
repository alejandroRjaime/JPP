// =============================================================================
// Recovering the RNG seed of the published dataset.
//
// ~/ssb_synth was not produced by datagen/GenerateSSB.scala: its keys and its
// `amount` column are drawn from a random generator, not derived from the row
// id. The dataset is therefore reproducible only if the seed can be recovered.
//
// The fact table is written as 2000 contiguous partitions of 300,000 rows, so
// row 0 is the first row of partition 0, and its `amount` is the first value the
// generator produced. Spark seeds `rand(seed)` per partition as
// `new XORShiftRandom(seed + partitionIndex)`, so for partition 0 the first
// value is a pure function of the seed alone. That makes a bounded search
// possible.
//
// Two generators are searched, because the dataset might not have come from
// Spark's `rand()` at all:
//
//   xorshift   Spark's XORShiftRandom, what `rand(seed)` uses.
//   javautil   java.util.Random, what a hand-written UDF would most likely use.
//
// The implementation of XORShiftRandom is replicated here rather than imported
// because Spark marks it private[spark]. It is validated against Spark itself
// before any search result is believed — see `verify` mode and
// scripts/seed_search.sh.
//
// Usage:
//   ./scripts/jpp SeedSearch verify  <seed> <partition> <count>
//   ./scripts/jpp SeedSearch search  <target> <from> <to> [tolerance]
// =============================================================================

import java.nio.ByteBuffer
import scala.util.hashing.MurmurHash3

object SeedSearch {

  // --- Spark's XORShiftRandom, replicated -----------------------------------
  final class XORShift(init: Long) {
    private var s: Long = XORShift.hashSeed(init)

    private def next(bits: Int): Int = {
      var n = s ^ (s << 21)
      n ^= (n >>> 35)
      n ^= (n << 4)
      s = n
      (n & ((1L << bits) - 1)).toInt
    }

    // java.util.Random.nextDouble, which XORShiftRandom inherits.
    def nextDouble(): Double =
      (((next(26).toLong) << 27) + next(27)) * (1.0 / (1L << 53).toDouble)
  }

  /** Same generator without the MurmurHash3 seed scramble, kept as a calibration
    * variant: older Spark releases seeded XORShiftRandom directly. */
  final class XORShiftRaw(init: Long) {
    private var s: Long = init
    private def next(bits: Int): Int = {
      var n = s ^ (s << 21)
      n ^= (n >>> 35)
      n ^= (n << 4)
      s = n
      (n & ((1L << bits) - 1)).toInt
    }
    def nextDouble(): Double =
      (((next(26).toLong) << 27) + next(27)) * (1.0 / (1L << 53).toDouble)
  }

  object XORShift {
    def hashSeed(seed: Long): Long = {
      val bytes    = ByteBuffer.allocate(8).putLong(seed).array()
      val lowBits  = MurmurHash3.bytesHash(bytes)
      val highBits = MurmurHash3.bytesHash(bytes, lowBits)
      (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
    }
  }

  // Spark's own XORShiftRandom, reached by reflection.
  //
  // The class is `private[spark]` in Scala, which is a compile-time restriction
  // only: the bytecode is public. Reaching it this way removes the risk that a
  // hand-written replica diverges from the implementation actually used — the
  // first replica attempted here did diverge, and a seed search built on it
  // would have produced a confident and meaningless negative.
  private val xorCtor = {
    val cls = Class.forName("org.apache.spark.util.random.XORShiftRandom")
    cls.getConstructor(java.lang.Long.TYPE)
  }
  private val nextDoubleM =
    Class.forName("java.util.Random").getMethod("nextDouble")

  private def sparkFirst(seed: Long): Double = {
    val rng = xorCtor.newInstance(Long.box(seed))
    nextDoubleM.invoke(rng).asInstanceOf[java.lang.Double].doubleValue()
  }

  private def xorshiftFirst(seed: Long): Double = sparkFirst(seed)
  private def javaFirst(seed: Long): Double     = new java.util.Random(seed).nextDouble()

  def main(args: Array[String]): Unit = {
    val mode = if (args.nonEmpty) args(0) else "verify"

    mode match {
      // Print what this implementation predicts, so it can be diffed against
      // what Spark actually produces for the same seed and partition.
      case "verify" =>
        val seed = if (args.length > 1) args(1).toLong else 12345L
        val part = if (args.length > 2) args(2).toInt  else 0
        val n    = if (args.length > 3) args(3).toInt  else 5
        val rng  = new XORShift(seed + part)
        println(s"predicted xorshift seed=$seed partition=$part")
        (0 until n).foreach(i => println(f"  [$i] ${rng.nextDouble()}%.17f"))
        val jr = new java.util.Random(seed + part)
        println(s"predicted java.util.Random seed=${seed + part}")
        (0 until n).foreach(i => println(f"  [$i] ${jr.nextDouble()}%.17f"))

      // Which construction actually reproduces Spark's `rand(seed)`. The first
      // attempt at replicating XORShiftRandom did not match, so the variant is
      // determined empirically against values Spark produced rather than from
      // recollection of its source.
      case "calibrate" =>
        val target = args(1).toDouble       // Spark's first value for (seed, partition)
        val seed   = if (args.length > 2) args(2).toLong else 12345L
        val part   = if (args.length > 3) args(3).toInt  else 0
        val init   = seed + part
        def hit(x: Double) = if (x == target) "  <== MATCH" else ""
        val variants = Seq[(String, Double)](
          "Spark XORShiftRandom(seed+part)"    -> sparkFirst(init),
          "XORShift replica(hashSeed(s+p))"    -> new XORShift(init).nextDouble(),
          "XORShift(seed+part), no hashSeed"   -> { val r = new XORShiftRaw(init); r.nextDouble() },
          "java.util.Random(seed+part)"        -> new java.util.Random(init).nextDouble(),
          "java.util.Random(hashSeed(s+p))"    -> new java.util.Random(XORShift.hashSeed(init)).nextDouble(),
          "XORShift(hashSeed(seed)+part)"      -> { val r = new XORShiftRaw(XORShift.hashSeed(seed) + part); r.nextDouble() }
        )
        println(f"target = $target%.17f   (seed=$seed partition=$part)")
        variants.foreach { case (name, v) => println(f"  $name%-38s ${v}%.17f${hit(v)}") }

      case "search" =>
        val target = args(1).toDouble
        val from   = args(2).toLong
        val to     = args(3).toLong
        // Exact equality is the right test: the target is read back from a
        // double stored in Parquet, so a matching seed reproduces it bit for
        // bit. A tolerance is offered only for the derived-column case, where
        // the stored value is a truncated integer and the underlying draw is
        // known only to lie in an interval.
        val tol    = if (args.length > 4) args(4).toDouble else 0.0
        // The stored column is `rand(seed) * mult`, so the draw is scaled up to
        // the stored value rather than the stored value divided down. Dividing
        // would introduce a rounding step the original computation never
        // performed and could turn an exact match into a near miss.
        val mult   = if (args.length > 5) args(5).toDouble else 1.0

        println(s"searching seeds [$from, $to) for stored value $target (mult=$mult, tol=$tol)")
        var s = from
        var found = 0
        val t0 = System.nanoTime()
        while (s < to) {
          val x = xorshiftFirst(s) * mult
          if (if (tol == 0.0) x == target else math.abs(x - target) <= tol)
            { println(f"  MATCH Spark rand seed=$s  ->  $x%.17f"); found += 1 }
          val j = javaFirst(s) * mult
          if (if (tol == 0.0) j == target else math.abs(j - target) <= tol)
            { println(f"  MATCH java.util.Random seed=$s  ->  $j%.17f"); found += 1 }
          s += 1
        }
        val dt = (System.nanoTime() - t0) / 1e9
        println(f"searched ${to - from}%,d seeds in $dt%.1f s; $found match(es)")

      // Ground truth. Asks Spark for the first values of `rand(seed)` over a
      // range partitioned exactly as the fact table is, so that `verify` can be
      // diffed against it. Without this step a search result would rest on an
      // unvalidated reimplementation of a private Spark class.
      case "sparkgen" =>
        val seed  = if (args.length > 1) args(1).toLong else 12345L
        val parts = if (args.length > 2) args(2).toInt  else 1
        val n     = if (args.length > 3) args(3).toInt  else 5
        val spark = org.apache.spark.sql.SparkSession.builder()
          .appName("SeedSearch-sparkgen")
          .master(sys.env.getOrElse("SPARK_MASTER", "local[2]"))
          .config("spark.sql.adaptive.enabled", "false")
          .getOrCreate()
        import org.apache.spark.sql.functions._
        val df = spark.range(0, n.toLong * parts, 1, parts)
          .withColumn("r", rand(seed))
          .withColumn("pid", spark_partition_id())
        println(s"spark rand(seed=$seed) over $parts partition(s)")
        df.orderBy("id").collect().foreach { row =>
          println(f"  id=${row.getLong(0)}%4d pid=${row.getInt(2)} r=${row.getDouble(1)}%.17f")
        }
        spark.stop()

      case other =>
        throw new IllegalArgumentException(s"unknown mode: $other")
    }
  }
}
