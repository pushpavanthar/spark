package org.apache.spark.sql.replay

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.functions.{col, hours}
import org.apache.spark.util.JsonProtocol

/**
 * Whole jobs, written and fed by this suite: the shapes batch pipelines have and the gate queries
 * lack (Iceberg writes with partition transforms, nested input, wide rollups, thousands of shuffle
 * partitions, a multi-input join pipeline). Self-contained: no external code, data or cluster.
 */
class SyntheticJobSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val tmp = Files.createTempDirectory("replay-jobs").toFile
  private val ends = TrieMap[Long, QueryExecution]()
  private var spark: SparkSession = _
  private var collector: Harness.Collector = _
  private val ns = "lake.jobs"

  override def beforeAll(): Unit = {
    spark = Harness.session(Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.lake" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.lake.type" -> "hadoop",
      "spark.sql.catalog.lake.warehouse" -> new File(tmp, "warehouse").toURI.toString,
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic"))
    spark.sparkContext.setLogLevel("ERROR")
    collector = Harness.attachCapture(spark)
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(e: SparkListenerEvent): Unit = e match {
        case end: SparkListenerSQLExecutionEnd if end.qe != null => ends(end.executionId) = end.qe
        case _ =>
      }
    })
    spark.sql(s"create namespace if not exists $ns")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** Every captured execution of `job` that shuffled, with the plan the run really ended with. */
  private def run(job: => Unit): Seq[(QueryCapture, SparkPlan)] = {
    val bus = spark.sparkContext.listenerBus
    bus.waitUntilEmpty()
    collector.events.clear()
    ends.clear()
    job
    bus.waitUntilEmpty(); bus.waitUntilEmpty()
    val events = collector.events.asScala.toSeq
      .map(e => JsonProtocol.sparkEventFromJson(JsonProtocol.sparkEventToJsonString(e)))
    val shuffles = events.collect { case s: SparkListenerSQLShuffleStageStats => s }
    events.collect { case p: SparkListenerSQLReplayPlan => p }.sortBy(_.executionId)
      .map(p => QueryCapture(p, shuffles.filter(_.executionId == p.executionId)))
      .filter(_.shuffles.nonEmpty)
      .map(c => c -> ends(c.plan.executionId).executedPlan)
  }

  private val whatIfs = Seq(
    Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "256k"),
    Map("spark.sql.adaptive.coalescePartitions.enabled" -> "false"),
    Map("spark.sql.shuffle.partitions" -> "200"),
    Map("spark.sql.shuffle.partitions" -> "200",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "256k"),
    Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"))

  /** Baseline replay must give the recorded plan; a what-if must give the real rerun or a flag. */
  private def check(conf: Map[String, String])(job: => Unit): Unit = {
    def set(extra: Map[String, String]): Unit = {
      (whatIfs.flatMap(_.keys) ++ conf.keys).distinct.foreach(spark.conf.unset)
      (conf ++ extra).foreach { case (k, v) => spark.conf.set(k, v) }
    }
    try {
      set(Map())
      val base = run(job)
      assert(base.nonEmpty, "the job produced no captured execution with a shuffle")
      for ((cap, real) <- base) {
        val (plan, flags) = Replay.replay(spark, cap)
        info(s"baseline ${Sig.sig(real)}")
        assert(Sig.sig(plan) == Sig.sig(real))
        assert(flags.isEmpty && cap.plan.opaqueNodes == 0, flags)
      }
      val cap = base.last._1
      for (w <- whatIfs) {
        set(w)
        val (plan, flags) = Replay.replay(spark, cap)
        val real = Sig.sig(run(job).last._2)
        val verdict = if (Sig.sig(plan) == real) "exact" else "MISS"
        info(s"what-if $w: $verdict, flags ${flags.mkString(",")}; real $real")
        assert(Sig.sig(plan) == real || flags.nonEmpty, s"$w replay ${Sig.sig(plan)}")
      }
    } finally {
      conf.keys.foreach(spark.conf.unset)
    }
  }

  test("nested events exploded and aggregated into an hourly-partitioned Iceberg table") {
    val inputs = Seq("r1", "r2", "r3").map(r => new File(tmp, s"auction/$r").getPath)
    for ((path, i) <- inputs.zipWithIndex) {
      spark.sql(s"""
        select 1762750800000 + id % 3600000 ts_ms, concat('dev', id % 50000) device,
          concat('app', id % 900) app, id % 2000 site, id % 6 os, id % 7 = 0 rewarded,
          if(id % 10 = 0, null, array(named_struct('buyer', id % 40, 'price', rand() * 5,
            'creative', named_struct('id', concat('c', id % 300))))) winners,
          transform(sequence(1, 1 + cast(id % 4 as int)), i -> named_struct(
            'buyer', (id + i) % 40,
            'bid', if((id + i) % 3 = 0, null, named_struct('creative', concat('c', id % 300),
              'price', rand() * 4)))) bids,
          map(cast(id % 40 as string), named_struct('floor', 0.5D)) floors, 0.1D default_floor
        from range(${i * 100000L}, ${(i + 1) * 100000L}, 1, 4)""").write.mode("overwrite").parquet(path)
    }
    def query = {
      spark.read.parquet(inputs: _*).createOrReplaceTempView("auction")
      spark.sql("""
        select cast(from_unixtime(a.ts_ms / 1000) as timestamp) event_time, upper(a.device) device,
          a.app, a.site, a.os, a.rewarded,
          case when a.app rlike '^app[0-9]+$' then a.app end numbered_app,
          a.winners[0].buyer win_buyer, a.winners[0].price win_price,
          a.winners[0].creative.id win_creative,
          to_json(collect_set(named_struct('buyer', b.buyer, 'creative', b.bid.creative,
            'price', b.bid.price, 'floor', cast(coalesce(
              element_at(a.floors, cast(b.buyer as string)).floor, a.default_floor) as double)))) bids,
          timestamp'2026-11-10 05:00:00' process_time
        from auction a lateral view outer explode(a.bids) x as b
        where a.winners is not null and size(a.winners) >= 1 and b.bid is not null
        group by cast(from_unixtime(a.ts_ms / 1000) as timestamp), upper(a.device), a.app, a.site,
          a.os, a.rewarded, a.winners[0].buyer, a.winners[0].price, a.winners[0].creative.id""")
    }
    val table = s"$ns.auction_landscape"
    query.limit(0).writeTo(table).using("iceberg").partitionedBy(hours(col("process_time"))).create()
    check(Map("spark.sql.shuffle.partitions" -> "1600",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "256MB")) {
      query.writeTo(table).overwritePartitions()
    }
  }

  test("wide rollup of one day from Iceberg into Iceberg at 12000 shuffle partitions") {
    val dims = (1 to 60).map(i => s"dim_$i")
    val measures = (1 to 20).map(i => s"m_$i")
    val ddl = (Seq("process_time timestamp", "event_time timestamp") ++ dims.map(_ + " string") ++
      measures.map(_ + " double")).mkString(", ")
    val (source, target) = (s"$ns.facts_hourly", s"$ns.facts_daily")
    Seq(source, target).foreach(t =>
      spark.sql(s"create table $t ($ddl) using iceberg partitioned by (days(process_time))"))
    spark.sql(s"""insert into $source select timestamp'2026-11-10 00:00:00',
      timestamp_seconds(1762732800 + id % 172800),
      ${dims.indices.map(i => s"cast(pmod(hash(id div 4, $i), 3) as string)").mkString(", ")},
      ${measures.map(_ => "cast(id % 7 as double)").mkString(", ")}
      from range(0, 400000, 1, 8)""")
    check(Map("spark.sql.shuffle.partitions" -> "12000")) {
      spark.sql(s"""insert overwrite $target
        select timestamp'2026-11-10 00:00:00',
          cast(concat(substring(event_time, 1, 10), ' 00:00:00') as timestamp),
          ${dims.mkString(", ")}, ${measures.map(m => s"sum($m)").mkString(", ")}
        from $source where process_time = '2026-11-10 00:00:00'
        group by 2, ${dims.indices.map(_ + 3).mkString(", ")}""")
    }
  }

  test("three regional inputs unioned, joined to dimensions and a skewed fact, then summarised") {
    def path(n: String) = new File(tmp, s"summary/$n").getPath
    for ((r, i) <- Seq("r1", "r2", "r3").zipWithIndex) {
      // Half of the requests belong to one site: the join with site_day is skewed.
      spark.sql(s"""select id, if(id % 2 = 0, 0, id % 20000) site, id % 400 model, id % 6 os,
          concat('dev', id % 80000) device, rand() bid, 1762750800 + id % 3600 ts
        from range(${i * 300000L}, ${(i + 1) * 300000L}, 1, 4)""")
        .write.mode("overwrite").parquet(path(r))
      spark.read.parquet(path(r)).createOrReplaceTempView(s"requests_$r")
    }
    spark.sql("select id model, concat('maker', id % 60) maker from range(400)")
      .write.mode("overwrite").parquet(path("model"))
    spark.sql("select id os, concat('os', id) os_name from range(6)")
      .write.mode("overwrite").parquet(path("os"))
    spark.sql("""select id % 20000 site, id div 20000 day, concat('pub', id % 3000) publisher,
        repeat('x', 40) note from range(200000)""").write.mode("overwrite").parquet(path("site_day"))
    Seq("model", "os", "site_day").foreach(t => spark.read.parquet(path(t)).createOrReplaceTempView(t))
    check(Map("spark.sql.shuffle.partitions" -> "600",
      "spark.sql.autoBroadcastJoinThreshold" -> "1m",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "2m",
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "1m",
      "spark.sql.adaptive.skewJoin.skewedPartitionFactor" -> "3")) {
      spark.sql("""
        with requests as (select * from requests_r1 union all select * from requests_r2
          union all select * from requests_r3)
        select from_unixtime(r.ts - r.ts % 600) bucket, m.maker, o.os_name, s.publisher,
          count(*) requests, sum(r.bid) bid, approx_count_distinct(r.device) devices
        from requests r join model m on r.model = m.model left join os o on r.os = o.os
          join site_day s on r.site = s.site and s.day < 3
        group by 1, 2, 3, 4""").write.mode("overwrite").parquet(path("out"))
    }
  }
}
