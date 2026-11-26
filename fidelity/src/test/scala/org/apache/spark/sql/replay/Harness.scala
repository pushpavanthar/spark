// Fidelity harness: real runs are the ground truth. For each query, one baseline run is captured;
// each config variant is then really executed and also replayed from the baseline capture, and the
// two final plans are compared.
package org.apache.spark.sql.replay

import java.io.{File, PrintWriter}
import java.nio.file.Files

import scala.io.Source

import scala.collection.JavaConverters._

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.util.JsonProtocol

case class Score(pairs: Int, exact: Int, joinsOk: Int, flagged: Int, unflaggedMisses: Int,
    medianMs: Double, maxMs: Double) {
  def exactRate: Double = exact.toDouble / pairs
  override def toString: String =
    f"exact=$exact/$pairs (${exactRate * 100}%.1f%%) joins=$joinsOk/$pairs flagged=$flagged " +
      f"unflaggedMisses=$unflaggedMisses replayMs median=$medianMs%.1f max=$maxMs%.1f"
}

object Harness {
  val base: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.shuffle.partitions" -> "64",
    "spark.sql.autoBroadcastJoinThreshold" -> "1m",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "4m",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "2m",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor" -> "3")

  // Config variants replayed against the baseline capture. Each is a what-if on top of `base`.
  val variants: Seq[(String, Map[String, String])] = Seq(
    "A_base" -> Map(),
    "bjt_off" -> Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"),
    "bjt_16m" -> Map("spark.sql.autoBroadcastJoinThreshold" -> "16m"),
    "bjt_64m" -> Map("spark.sql.autoBroadcastJoinThreshold" -> "64m"),
    "aqe_bjt_64m" -> Map("spark.sql.adaptive.autoBroadcastJoinThreshold" -> "64m"),
    "advisory_16m" -> Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "16m"),
    "advisory_1m" -> Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1m"),
    "skew_off" -> Map("spark.sql.adaptive.skewJoin.enabled" -> "false"),
    "skew_thr_64m" -> Map("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "64m"),
    "skew_factor_50" -> Map("spark.sql.adaptive.skewJoin.skewedPartitionFactor" -> "50"),
    "skew_thr_8m" -> Map("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "8m"),
    "skew_adv_16m" -> Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "16m",
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "8m"),
    "skew_adv_1m" -> Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1m"),
    "local_read_off" -> Map("spark.sql.adaptive.localShuffleReader.enabled" -> "false"),
    "prefer_shj" -> Map("spark.sql.join.preferSortMergeJoin" -> "false"),
    "coalesce_off" -> Map("spark.sql.adaptive.coalescePartitions.enabled" -> "false"),
    "parts_32" -> Map("spark.sql.shuffle.partitions" -> "32"),
    "parts_16_bjt_16m" -> Map("spark.sql.shuffle.partitions" -> "16",
      "spark.sql.autoBroadcastJoinThreshold" -> "16m"),
    "parts_8_adv_16m" -> Map("spark.sql.shuffle.partitions" -> "8",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "16m"),
    "parts_48" -> Map("spark.sql.shuffle.partitions" -> "48"),
    "parts_128" -> Map("spark.sql.shuffle.partitions" -> "128"))

  val queries: Seq[(String, String)] = Seq(
    "q1_dims" -> """select i.category, sum(s.price) from sales s join item i on s.item_sk = i.item_sk
      join dates d on s.date_sk = d.date_sk where d.year = 2001 group by i.category""",
    "q2_filtered_dim" -> """select c.state, count(*) from sales s join cust c
      on s.cust_sk = c.cust_sk where c.segment = 7 group by c.state""",
    "q3_skew" -> """select v.wh, sum(s.qty) from sales s join inv v on s.item_sk = v.item_sk
      group by v.wh""",
    "q4_agg_join" -> """select c.state, sum(t.tot) from (select cust_sk, sum(price) tot from sales
      group by cust_sk) t join cust c on t.cust_sk = c.cust_sk group by c.state""",
    "q5_empty" -> """select i.brand, count(*) from sales s join item i on s.item_sk = i.item_sk
      where i.category = -1 group by i.brand""",
    "q6_skew_dims" -> """select i.brand, v.wh, sum(s.qty) from sales s
      join inv v on s.item_sk = v.item_sk join item i on s.item_sk = i.item_sk
      where i.category = 3 group by i.brand, v.wh""",
    // pad is incompressible, so the hot item_sk is skewed in bytes, not only in rows
    "q7_byte_skew" -> """select v.wh, max(s.pad), sum(s.qty) from sales s join inv v
      on s.item_sk = v.item_sk group by v.wh""",
    "q8_byte_skew_dim" -> """select i.category, v.wh, max(s.pad) from sales s
      join inv v on s.item_sk = v.item_sk join item i on s.item_sk = i.item_sk
      group by i.category, v.wh""",
    "q9_clustered_skew" -> """select v.wh, max(s.pad), sum(s.qty) from sales_c s join inv v
      on s.item_sk = v.item_sk group by v.wh""")

  def gen(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._
    val n = 4000000L
    spark.range(n).selectExpr(
      "case when id % 5 = 0 then 1 else (id * 7919) % 200000 end as item_sk",
      "(id * 104729) % 500000 as cust_sk", "id % 366 as date_sk",
      "cast(id % 10 as int) as qty", "cast((id % 1000) / 10.0 as double) as price",
      "sha2(cast(id as string), 256) as pad")
      .repartition(8).write.mode("overwrite").parquet(s"$dir/sales")
    // same rows, clustered by key: the hot item_sk sits in one input file, so mappers are uneven
    spark.read.parquet(s"$dir/sales").repartitionByRange(8, $"item_sk")
      .write.mode("overwrite").parquet(s"$dir/sales_c")
    spark.range(200000).selectExpr("id as item_sk", "cast(id % 50 as int) as category",
      "concat('brand_', id % 500) as brand").repartition(2)
      .write.mode("overwrite").parquet(s"$dir/item")
    spark.range(500000).selectExpr("id as cust_sk", "concat('st_', id % 50) as state",
      "cast(id % 100 as int) as segment").repartition(2)
      .write.mode("overwrite").parquet(s"$dir/cust")
    spark.range(366).selectExpr("id as date_sk", "cast(2000 + id % 3 as int) as year")
      .repartition(1).write.mode("overwrite").parquet(s"$dir/dates")
    spark.range(800000).selectExpr("id % 200000 as item_sk", "cast(id % 4 as int) as wh",
      "cast(id % 100 as int) as onhand").repartition(2)
      .write.mode("overwrite").parquet(s"$dir/inv")
  }

  def setConf(spark: SparkSession, c: Map[String, String]): Unit = {
    variants.flatMap(_._2.keys).distinct.foreach(spark.conf.unset)
    (base ++ c).foreach { case (k, v) => spark.conf.set(k, v) }
  }

  def finalPlan(qe: QueryExecution): SparkPlan = qe.executedPlan match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case o => o
  }

  /** Collects the replay events capture produces. */
  class Collector {
    val events = new java.util.concurrent.ConcurrentLinkedQueue[SparkListenerEvent]()
  }

  /**
   * Runs the query and returns what the capture listener wrote for it, after the same JSON
   * round trip an event log imposes. Nothing of the in-memory plan reaches the replay.
   */
  def runAndCapture(spark: SparkSession, collector: Collector, sql: String): (QueryCapture, SparkPlan) = {
    val bus = spark.sparkContext.listenerBus
    bus.waitUntilEmpty()
    collector.events.clear()
    val df = spark.sql(sql)
    df.collect()
    bus.waitUntilEmpty()
    val events = collector.events.asScala.toSeq
      .map(e => JsonProtocol.sparkEventFromJson(JsonProtocol.sparkEventToJsonString(e)))
    val plan = events.collect { case p: SparkListenerSQLReplayPlan => p }.maxBy(_.executionId)
    val shuffles = events.collect {
      case s: SparkListenerSQLShuffleStageStats if s.executionId == plan.executionId => s
    }
    (QueryCapture(plan, shuffles), finalPlan(df.queryExecution))
  }

  /** Bump when `gen` changes. */
  val dataVersion = 1

  /** Cached data and ground truth are only valid for the harness definition that produced them. */
  val version: String = Integer.toHexString((queries, variants, base, dataVersion).hashCode)

  val tables: Seq[String] = Seq("sales", "sales_c", "item", "cust", "dates", "inv")

  def session(conf: Map[String, String] = Map(), cores: Int = 4): SparkSession = {
    val b = SparkSession.builder().master(s"local[$cores]").appName("replay-fidelity")
      .config("spark.ui.enabled", "false").config("spark.sql.sources.useV1SourceList", "parquet")
    conf.foreach { case (k, v) => b.config(k, v) }
    b.withExtensions(Replay.install _).getOrCreate()
  }

  /**
   * Really runs the workload with event logging: once to warm the JVM, then once under the job
   * group, at the harness base configuration plus `variant`. Returns the event log.
   */
  def runWithEventLog(cores: Int, group: String, conf: Map[String, String] = Map(),
      variant: Map[String, String] = Map()): String = {
    val logDir = Files.createTempDirectory(s"replay-log-$cores").toFile
    val spark = session(conf ++ Map("spark.eventLog.enabled" -> "true",
      "spark.eventLog.dir" -> logDir.toURI.toString), cores)
    spark.sparkContext.setLogLevel("ERROR")
    try {
      prepare(spark)
      setConf(spark, variant)
      def workload(): Unit = queries.foreach { case (_, sql) => spark.sql(sql).collect() }
      workload()
      spark.sparkContext.setJobGroup(group, "measured workload")
      workload()
    } finally spark.stop()
    logDir.listFiles().filter(f => f.isFile && f.getName.startsWith("local-")).head.getPath
  }

  /** Captures every query of the session into a collector. */
  def attachCapture(spark: SparkSession): Collector = {
    val collector = new Collector
    spark.sparkContext.addSparkListener(new ReplayCapturer(e => collector.events.add(e)))
    collector
  }

  def registerTables(spark: SparkSession, dir: String): Unit =
    tables.foreach(t => spark.read.parquet(s"$dir/$t").createOrReplaceTempView(t))

  /** Generates data and ground truth on first use; returns the directory they are cached in. */
  def prepare(spark: SparkSession): File = {
    val dir = new File(sys.props.getOrElse("fidelity.dir", "target/fidelity"), version)
    dir.mkdirs()
    val marker = new File(dir, "_generated")
    if (!marker.exists) { gen(spark, dir.getPath); marker.createNewFile() }
    registerTables(spark, dir.getPath)
    val truthFile = new File(dir, "truth.tsv")
    if (!truthFile.exists) writeTruth(spark, truthFile)
    dir
  }

  /** Really runs every query under every variant and records the final plan signature. */
  def writeTruth(spark: SparkSession, file: File): Unit = {
    val tmp = new File(file.getPath + ".tmp")
    val w = new PrintWriter(tmp)
    try {
      for ((q, sql) <- queries; (v, c) <- variants) {
        setConf(spark, c)
        val df = spark.sql(sql)
        df.collect()
        w.println(s"$q\t$v\t${Sig.sig(finalPlan(df.queryExecution))}")
      }
    } finally w.close()
    require(tmp.renameTo(file), s"cannot move $tmp to $file")
  }

  def loadTruth(file: File): Map[(String, String), String] = {
    val src = Source.fromFile(file)
    try src.getLines().map(_.split("\t")).map(a => (a(0), a(1)) -> a(2)).toMap finally src.close()
  }

  /** Captures the baseline run of each query, replays every variant, compares with the truth. */
  def eval(spark: SparkSession, collector: Collector, truth: Map[(String, String), String],
      detailFile: File): Score = {
    var exact = 0; var joinsOk = 0; var total = 0; var flagged = 0; var unflaggedMisses = 0
    val times = scala.collection.mutable.ArrayBuffer[Double]()
    val detail = new PrintWriter(detailFile)
    try {
      for ((q, sql) <- queries) {
        setConf(spark, Map())
        val (cap, _) = runAndCapture(spark, collector, sql)
        for ((v, c) <- variants) {
          setConf(spark, c)
          val t0 = System.nanoTime()
          val (got, flags) = try {
            val (p, f) = Replay.replay(spark, cap)
            (Sig.sig(p), f)
          } catch { case e: Exception => ("CRASH " + e.toString.take(300), Seq("CRASH")) }
          val ms = (System.nanoTime() - t0) / 1e6
          times += ms
          val want = truth((q, v))
          val ok = got == want
          total += 1
          if (ok) exact += 1
          if (Sig.joins(got) == Sig.joins(want) && !got.startsWith("CRASH")) joinsOk += 1
          if (flags.nonEmpty) flagged += 1
          if (!ok && flags.isEmpty) unflaggedMisses += 1
          detail.println(
            s"${if (ok) "OK  " else "MISS"}\t$q\t$v\t${ms.toInt}ms\tflags=${flags.mkString(",")}")
          if (!ok) { detail.println(s"   want $want"); detail.println(s"   got  $got") }
        }
      }
    } finally detail.close()
    val sorted = times.sorted
    Score(total, exact, joinsOk, flagged, unflaggedMisses, sorted(sorted.size / 2), sorted.last)
  }
}
