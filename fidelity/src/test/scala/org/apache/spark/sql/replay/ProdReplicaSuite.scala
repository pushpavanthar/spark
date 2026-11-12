package org.apache.spark.sql.replay

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.sys.process._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.util.JsonProtocol

/**
 * Local replicas of production jobs: the job's own SQL and SQL configs, read from the local
 * checkouts of the job repositories, run in local mode on synthetic data against a local Iceberg
 * catalog. Nothing here reads production data or touches a production DAG. Cancelled when the
 * checkouts are absent (REPLICA_SRC, default ~/inmobi/repo/marketing).
 *
 * Synthetic: row counts, column types of the rollup tables, table partitioning.
 */
class ProdReplicaSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val src = new File(sys.env.getOrElse("REPLICA_SRC",
    sys.props("user.home") + "/inmobi/repo/marketing"))
  private val dags = new File(src, "exchange-gcp-airflow-dags")
  private val tmp = Files.createTempDirectory("replay-replica").toFile
  private val ends = TrieMap[Long, QueryExecution]()
  private var spark: SparkSession = _
  private var collector: Harness.Collector = _

  override def beforeAll(): Unit = {
    spark = Harness.session(Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.iceberg_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.iceberg_catalog.type" -> "hadoop",
      "spark.sql.catalog.iceberg_catalog.warehouse" -> new File(tmp, "warehouse").toURI.toString,
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic"))
    spark.sparkContext.setLogLevel("ERROR")
    collector = Harness.attachCapture(spark)
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(e: SparkListenerEvent): Unit = e match {
        case end: SparkListenerSQLExecutionEnd if end.qe != null => ends(end.executionId) = end.qe
        case _ =>
      }
    })
    spark.sql("create namespace if not exists iceberg_catalog.replica")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def read(f: File): String = { val s = Source.fromFile(f); try s.mkString finally s.close() }

  /** The runtime SQL configs the DAG sets for the job (commented-out lines excluded). */
  private def sqlConf(dag: String): Map[String, String] = {
    val conf = "\"(spark\\.sql\\.(?:adaptive|shuffle|files|autoBroadcast)[\\w.]*)\":\\s*\"([^\"]*)\"".r
    read(new File(dags, s"dags/gke/$dag/task_trigger_spark_task.py")).linesIterator
      .filterNot(_.trim.startsWith("#")).flatMap(conf.findFirstMatchIn)
      .map(m => m.group(1) -> m.group(2)).toMap
  }

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
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "256k"))

  /** Baseline replay must give the recorded plan; a what-if must give the real rerun or a flag. */
  private def check(dag: String)(job: => Unit): Unit = {
    val conf = sqlConf(dag)
    def set(extra: Map[String, String]): Unit = {
      whatIfs.flatMap(_.keys).distinct.foreach(spark.conf.unset)
      (conf ++ extra).foreach { case (k, v) => spark.conf.set(k, v) }
    }
    set(Map())
    val base = run(job)
    assert(base.nonEmpty, "the job produced no captured execution with a shuffle")
    for ((cap, real) <- base) {
      val (plan, flags) = Replay.replay(spark, cap)
      info(s"conf $conf")
      info(s"baseline ${Sig.sig(real)}; opaque nodes ${cap.plan.opaqueNodes}; flags $flags")
      assert(Sig.sig(plan) == Sig.sig(real))
      assert(flags.isEmpty && cap.plan.opaqueNodes == 0, flags)
    }
    val cap = base.last._1
    for (w <- whatIfs) {
      set(w)
      val (plan, flags) = Replay.replay(spark, cap)
      val real = Sig.sig(run(job).last._2)
      info(s"what-if $w: real $real; replay ${Sig.sig(plan)}; flags $flags")
      assert(Sig.sig(plan) == real || flags.nonEmpty, w)
    }
  }

  test("dm_bid_landscape: PySpark SQL job writing an Iceberg table") {
    val script = new File(src, "gcp-exchange-pyspark/custom-jobs/ix_bid_landscape.py")
    assume(script.exists && dags.exists, s"no checkout under $src")
    val sql = "(?s)spark\\.sql\\(f\"\"\"(.*?)\"\"\"\\)".r.findAllMatchIn(read(script)).map(_.group(1))
      .maxBy(_.length).replace("{process_time}", "2026-11-10 05:00:00")
    val inputs = Seq("ams", "eus", "sgp").map(r => new File(tmp, s"supply_rr/$r").getPath)
    for ((path, i) <- inputs.zipWithIndex) {
      spark.sql(s"""
        select 1762750800000 + id % 3600000 timestampMs, concat('maid', id % 50000) rawMaid,
          concat('u', id % 4) selectedUidType, concat('t', id % 5) requestedAdType,
          cast(320 + id % 3 as int) resolvedSlotWidth, cast(50 + id % 3 as int) resolvedSlotHeight,
          id % 7 = 0 isRewardedSlot, concat('b', id % 900) bundleId,
          if(id % 2 = 0, cast(id % 900 as string), concat('x', id % 900)) appBundleIdCas,
          id % 2000 siteIncId, concat('i', id % 3) inventoryType, concat('10.0.', id % 250) ipAddress,
          id % 400 handsetModelId, id % 6 operatingSystemId, concat('v', id % 30) detailedOsVersion,
          id % 60 manufacturerId,
          if(id % 10 = 0, null, array(named_struct('dspIncId', id % 40, 'adUnitEcpmBid', rand() * 5,
            'casSpecificInfo', named_struct('partnerCreativeId', concat('c', id % 300)))))
            winningAdUnits,
          transform(sequence(1, 1 + cast(id % 4 as int)), i -> named_struct(
            'forwardInfo', named_struct('dspIncId', (id + i) % 40),
            'fillInfo', if((id + i) % 3 = 0, null, named_struct('creativeId', concat('c', id % 300),
              'creativeAdomainIncId', id % 500, 'netBid', rand() * 4)))) channels,
          map(cast(id % 40 as string), named_struct('mabAttributes',
            named_struct('dynamicFloorPrice', 0.5D))) dspFloorAttributesMap,
          0.1D auctionBidFloor
        from range(${i * 100000L}, ${(i + 1) * 100000L}, 1, 4)""").write.mode("overwrite").parquet(path)
    }
    val table = "iceberg_catalog.replica.ix_bid_landscape"
    def query = {
      spark.read.parquet(inputs: _*).createOrReplaceTempView("supply_rr")
      spark.sql(sql)
    }
    import org.apache.spark.sql.functions.{col, hours}
    // Assumed: the production table is partitioned on the hour of process_time.
    query.limit(0).writeTo(table).using("iceberg").partitionedBy(hours(col("process_time"))).create()
    check("datamesh/dm_bid_landscape")(query.writeTo(table).overwritePartitions())
  }

  private def rollup(dag: String, name: String): Unit = {
    val macroFile = new File(src,
      "exchange-dbt-applications/exchange_dbt_applications/macros/rollup_query.sql")
    val configFile = new File(dags, s"configs/rollups/$name.json")
    assume(macroFile.exists && configFile.exists, s"no checkout under $src")
    def list(key: String): Seq[String] =
      (s""""$key"\\s*:\\s*\\[(.*?)\\]""").r.findFirstMatchIn(read(configFile).replace("\n", " "))
        .toSeq.flatMap(m => "\"(\\w+)\"".r.findAllMatchIn(m.group(1)).map(_.group(1).toLowerCase))
    def one(key: String): String =
      (s""""$key"\\s*:\\s*"(\\w+)"""").r.findFirstMatchIn(read(configFile)).get.group(1).toLowerCase
    val pt = one("processTimeColumn")
    val times = Seq(one("eventTimeColumn"), one("requestTimeColumn"))
    val (dims, measures) = (list("dimensionColumns"), list("measureColumns"))
    // Assumed column types: dimensions string, measures double.
    val columns = (pt +: times).map(_ -> "timestamp") ++ dims.map(_ -> "string") ++
      measures.map(_ -> "double")
    val ddl = columns.map { case (n, t) => s"$n $t" }.mkString(", ")
    val (source, target) = (s"iceberg_catalog.replica.${name}_src", s"iceberg_catalog.replica.$name")
    Seq(source, target).foreach(t =>
      spark.sql(s"create table $t ($ddl) using iceberg partitioned by (days($pt))"))
    val day = "2026-11-10 00:00:00"
    spark.sql(s"""insert into $source select timestamp'$day',
      ${times.map(_ => "timestamp_seconds(1762732800 + id % 172800)").mkString(", ")},
      ${dims.indices.map(i => s"cast(pmod(hash(id div 40, $i), 3) as string)").mkString(", ")}
      ${measures.map(_ => ", cast(id % 7 as double)").mkString}
      from range(0, 400000, 1, 8)""")
    val script = new File(getClass.getResource("/render_rollup.py").toURI).getPath
    val json = columns.map { case (n, t) => s"""{"name":"$n","dtype":"$t"}""" }.mkString("[", ",", "]")
    val select = Seq("python3", script, macroFile.getPath, configFile.getPath, json, source,
      s"$pt = '$day'").!!
    check(dag) {
      // What dbt-spark's insert_overwrite does for Iceberg: the model as a view, then the insert.
      spark.sql(select).createOrReplaceTempView(s"${name}__dbt_tmp")
      spark.sql(s"insert overwrite $target select ${columns.map(_._1).mkString(", ")} " +
        s"from ${name}__dbt_tmp")
    }
  }

  test("gold_daily_global_request_fact: dbt rollup into Iceberg") {
    rollup("supply/gold_daily_global_request_fact", "gold_daily_global_request_fact")
  }

  // Production runs this job on Comet; the Comet jar has no macOS native library, so the replica
  // runs on vanilla Spark and says nothing about capture under Comet.
  test("gold_unifid_daily_heavy_fact: dbt rollup into Iceberg (without Comet)") {
    rollup("exchange/gold_unifid_daily_heavy_fact", "gold_unifid_daily_heavy_fact")
  }
}
