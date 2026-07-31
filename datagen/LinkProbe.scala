// =============================================================================
// Measures what the paper needs to report about the storage link, rather than
// quoting the nominal interface speed: the actual size of the remote dataset,
// and the throughput of a read over the same path Spark uses.
//
//   S3_SECRET=... ./scripts/jpp LinkProbe s3a://ssb-benchmark/sf600
// =============================================================================

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

object LinkProbe {

  def main(args: Array[String]): Unit = {
    val base = if (args.nonEmpty) args(0) else "s3a://ssb-benchmark/sf600"

    val spark = SparkSession.builder()
      .appName("LinkProbe")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[4]"))
      .getOrCreate()

    val conf = spark.sparkContext.hadoopConfiguration
    val dir  = new Path(s"$base/lineorder")
    val fs   = dir.getFileSystem(conf)

    // --- size of the remote dataset -----------------------------------------
    val cs = fs.getContentSummary(dir)
    println(f"remote files  = ${cs.getFileCount}%,d")
    println(f"remote bytes  = ${cs.getLength}%,d  (${cs.getLength / 1e9}%.2f GB)")

    // --- single-stream throughput over the same path ------------------------
    val files = fs.listStatus(dir).filter(_.getPath.getName.endsWith(".parquet"))
    require(files.nonEmpty, s"no parquet files under $dir")
    val one = files.head.getPath
    println(f"probe file    = ${one.getName}  (${files.head.getLen}%,d bytes)")

    val buf = new Array[Byte](1 << 20)
    val t0  = System.nanoTime()
    val in  = fs.open(one)
    var n = 0L
    var r = in.read(buf)
    while (r > 0) { n += r; r = in.read(buf) }
    in.close()
    val dt = (System.nanoTime() - t0) / 1e9
    println(f"single stream = $n%,d bytes in $dt%.2f s = ${n / dt / 1e6}%.1f MB/s "
          + f"(${n / dt * 8 / 1e6}%.0f Mbit/s)")

    // --- parallel throughput, as the benchmark actually reads ---------------
    val sample = files.take(16)
    val t1 = System.nanoTime()
    val total = spark.sparkContext
      .parallelize(sample.map(_.getPath.toString), sample.length)
      .map { path =>
        val f  = new Path(path)
        val fs2 = f.getFileSystem(new org.apache.hadoop.conf.Configuration(conf))
        val b  = new Array[Byte](1 << 20)
        val s  = fs2.open(f)
        var c = 0L
        var k = s.read(b)
        while (k > 0) { c += k; k = s.read(b) }
        s.close(); c
      }.sum().toLong
    val dt1 = (System.nanoTime() - t1) / 1e9
    println(f"parallel x${sample.length}%d  = $total%,d bytes in $dt1%.2f s = "
          + f"${total / dt1 / 1e6}%.1f MB/s (${total / dt1 * 8 / 1e6}%.0f Mbit/s)")

    spark.stop()
  }
}
