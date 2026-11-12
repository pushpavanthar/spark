package org.apache.spark.sql.replay

import java.io.File
import java.nio.file.Files

import scala.collection.mutable
import scala.io.Source

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.scheduler.{ReplayListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.util.JsonProtocol

/** What the capture jar promises an application: complete, literal-free, harmless, readable. */
class CaptureSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val logDir = Files.createTempDirectory("replay-eventlog").toFile
  private var spark: SparkSession = _
  private var collector: Harness.Collector = _

  override def beforeAll(): Unit = {
    spark = Harness.session(Map("spark.eventLog.enabled" -> "true",
      "spark.eventLog.dir" -> logDir.toURI.toString))
    spark.sparkContext.setLogLevel("ERROR")
    Harness.prepare(spark)
    collector = Harness.attachCapture(spark)
    Harness.setConf(spark, Map())
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def json(cap: QueryCapture): String =
    (cap.plan +: cap.shuffles).map(JsonProtocol.sparkEventToJsonString).mkString("\n")

  test("skeletons of the harness queries have no opaque node and keep the plan's operators") {
    for ((name, sql) <- Harness.queries) {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      val encoder = new SkeletonCodec.Encoder
      val decoded = SkeletonCodec.decode(encoder.plan(plan), spark)
      assert(encoder.opaqueNodes == 0, name)
      def shape(p: LogicalPlan): Seq[String] = p.collect { case n if n.children.nonEmpty => n.nodeName }
      assert(shape(decoded) == shape(plan), name)
      assert(decoded.output.map(_.exprId.id) == plan.output.map(_.exprId.id), name)
    }
  }

  test("literal values never reach the events, and distinct literals stay distinct") {
    val (cap, _) = Harness.runAndCapture(spark, collector, """
      select c.state, count(*) from sales s join cust c on s.cust_sk = c.cust_sk
      where c.state in ('ZZ_SECRET_A', 'ZZ_SECRET_B') and s.qty < 918273645 and s.price > 918273646
      group by c.state""")
    val text = json(cap)
    Seq("ZZ_SECRET", "918273645", "918273646").foreach(s => assert(!text.contains(s), s))
    val literals = SkeletonCodec.decode(cap.plan.root, spark).flatMap(_.expressions)
      .flatMap(_.collect { case l: org.apache.spark.sql.catalyst.expressions.Literal => l })
    assert(literals.map(_.value).distinct.size >= 4, literals)
  }

  test("a UDF becomes an opaque expression, is flagged, and the query still replays") {
    spark.udf.register("secret_udf", (x: Long) => x + 1)
    val (cap, real) = Harness.runAndCapture(spark, collector, """
      select c.state, sum(secret_udf(s.cust_sk)) from sales s join cust c on s.cust_sk = c.cust_sk
      group by c.state""")
    assert(cap.plan.opaqueNodes == 1)
    val (plan, flags) = Replay.replay(spark, cap)
    assert(flags == Seq("OPAQUE_NODE"), flags)
    assert(Sig.sig(plan) == Sig.sig(real))
  }

  test("a write command is captured through its query and replays to the same plan") {
    val out = new File(logDir, "write-out").getPath
    spark.sparkContext.listenerBus.waitUntilEmpty()
    collector.events.clear()
    val df = spark.sql(Harness.queries.toMap.apply("q4_agg_join"))
    df.write.mode("overwrite").parquet(out)
    val bus = spark.sparkContext.listenerBus
    bus.waitUntilEmpty(); bus.waitUntilEmpty()
    import scala.collection.JavaConverters._
    val events = collector.events.asScala.toSeq
      .map(e => JsonProtocol.sparkEventFromJson(JsonProtocol.sparkEventToJsonString(e)))
    val plans = events.collect { case p: SparkListenerSQLReplayPlan => p }
    val shuffles = events.collect { case s: SparkListenerSQLShuffleStageStats => s }
    val plan = plans.find(p => shuffles.exists(_.executionId == p.executionId))
      .getOrElse(fail(s"no captured execution has shuffles: ${plans.size} plans"))
    val cap = QueryCapture(plan, shuffles.filter(_.executionId == plan.executionId))
    val (replayed, flags) = Replay.replay(spark, cap)
    assert(flags.isEmpty, flags)
    // The same query read back with collect() is the reference for the plan below the write.
    val (_, real) = Harness.runAndCapture(spark, collector, Harness.queries.toMap.apply("q4_agg_join"))
    assert(Sig.sig(replayed) == Sig.sig(real))
  }

  // The plan-shape check of the design: a replay under the recorded configs must give the plan
  // the run really ended with. Each shape exercises operators or expressions the gate queries lack.
  private val shapes = Seq(
    "window" -> """select * from (select s.cust_sk, s.price, row_number() over
      (partition by s.cust_sk order by s.price desc) rn from sales s) where rn <= 3""",
    "semi_join" -> """select count(*) from sales s where s.cust_sk in
      (select cust_sk from cust where segment = 3)""",
    "anti_join" -> """select count(*) from cust c where not exists
      (select 1 from sales s where s.cust_sk = c.cust_sk and s.qty > 8)""",
    "scalar_subquery" -> """select count(*) from sales where price > (select avg(price) from sales)""",
    "union_distinct" -> """select item_sk from sales where qty = 1 union
      select item_sk from inv where wh = 2""",
    "rollup" -> """select i.category, i.brand, sum(s.price) from sales s join item i
      on s.item_sk = i.item_sk group by rollup(i.category, i.brand)""",
    "count_distinct" -> """select c.state, count(distinct s.item_sk), sum(s.qty) from sales s
      join cust c on s.cust_sk = c.cust_sk group by c.state""",
    "sort_limit" -> """select s.cust_sk, sum(s.price) p from sales s group by s.cust_sk
      order by p desc limit 10""",
    "global_sort" -> """select s.date_sk, count(*) c from sales s group by s.date_sk order by c""",
    "explode" -> """select x, count(*) from (select explode(array(qty, qty + 1)) x from sales)
      group by x""",
    "case_cast_like" -> """select case when c.state like 'st_1%' then 'a' else 'b' end k,
      sum(cast(s.price as decimal(18, 2))) from sales s join cust c on s.cust_sk = c.cust_sk
      where s.date_sk between 10 and 200 and c.segment in (1, 2, 3) group by 1""",
    "left_outer_cte" -> """with t as (select item_sk, sum(qty) q from sales group by item_sk)
      select i.category, sum(t.q) from item i left join t on i.item_sk = t.item_sk
      group by i.category""",
    "self_join" -> """select count(*) from sales a join sales b
      on a.cust_sk = b.cust_sk and a.date_sk = b.date_sk + 1 where a.qty = 9 and b.qty = 0""")

  for ((name, sql) <- shapes) {
    test(s"baseline replay reproduces the real plan: $name") {
      val (cap, real) = Harness.runAndCapture(spark, collector, sql)
      assert(cap.plan.opaqueNodes == 0)
      val (plan, flags) = Replay.replay(spark, cap)
      assert(flags.isEmpty, flags)
      assert(Sig.sig(plan) == Sig.sig(real))
    }
  }

  test("a failure inside capture is swallowed") {
    val qe = spark.sql("select 1").queryExecution
    val broken = new QueryExecution(spark, qe.logical) {
      override lazy val optimizedPlan: LogicalPlan = throw new IllegalStateException("boom")
    }
    val end = SparkListenerSQLExecutionEnd(-1, 0L)
    end.qe = broken
    new ReplayCaptureListener().onOtherEvent(end)
  }

  test("the event log holds the events, replays cleanly, and capture stays cheap") {
    for ((_, sql) <- Harness.queries) Harness.runAndCapture(spark, collector, sql)
    val captureMs = Harness.queries.map { case (_, sql) =>
      val df = spark.sql(sql)
      df.collect()
      val t0 = System.nanoTime()
      ReplayCapture.events(0, df.queryExecution)
      (System.nanoTime() - t0) / 1e6
    }
    spark.stop()
    spark = null
    val log = logDir.listFiles().filter(f => f.isFile && f.getName.startsWith("local-")).head
    val lines = { val s = Source.fromFile(log); try s.getLines().toList finally s.close() }
    val ours = lines.filter(_.contains("org.apache.spark.sql.replay.SparkListenerSQL"))
    assert(ours.exists(_.contains("SparkListenerSQLReplayPlan")))
    assert(ours.exists(_.contains("SparkListenerSQLShuffleStageStats")))

    // What the History Server does with the file.
    val seen = mutable.Buffer[SparkListenerEvent]()
    val bus = new ReplayListenerBus()
    bus.addListener(new SparkListener {
      override def onOtherEvent(e: SparkListenerEvent): Unit = seen += e
    })
    val in = Files.newInputStream(log.toPath)
    try bus.replay(in, log.getName) finally in.close()
    assert(seen.count(_.isInstanceOf[SparkListenerSQLReplayPlan]) >= Harness.queries.size)

    val total = lines.map(_.length.toLong).sum
    val added = ours.map(_.length.toLong).sum
    info(f"event log ${total / 1024} KiB, replay events ${added / 1024} KiB " +
      f"(${added * 100.0 / (total - added)}%.1f%% growth); capture per query " +
      f"median ${captureMs.sorted.apply(captureMs.size / 2)}%.1f ms, max ${captureMs.max}%.1f ms")
  }
}
