package org.apache.spark.sql.replay

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

/**
 * A what-if trace against the truth: the workload really runs under the baseline and under each
 * candidate configuration; the baseline's event log is rewritten for the candidate and must have
 * the stages and task counts of the candidate's real run.
 */
class WhatIfTraceSuite extends AnyFunSuite {
  private type Obj = Map[String, Any]
  private val capture = Map("spark.extraListeners" -> classOf[ReplayCaptureListener].getName,
    // Executor settings mean nothing in local mode; the simulator reads the baseline shape there.
    "spark.executor.instances" -> "1", "spark.executor.cores" -> "4",
    "spark.executor.memory" -> "4g")

  private def stages(trace: Obj): Seq[Obj] = trace("stages").asInstanceOf[Seq[Obj]]
  private def taskCounts(trace: Obj): Seq[Int] =
    stages(trace).map(_("tasks").asInstanceOf[Seq[Obj]].size).sorted
  private def taskSeconds(trace: Obj): Double = stages(trace)
    .flatMap(_("tasks").asInstanceOf[Seq[Obj]])
    .map(_("duration_ms").asInstanceOf[Number].doubleValue).sum / 1000

  private lazy val baseline = Harness.runWithEventLog(4, "measure", capture)

  private def check(name: String, conf: Map[String, String], exact: Boolean): Unit = test(name) {
    val real = AroTrace.build(Harness.runWithEventLog(4, "measure", capture, conf))
    val log = baseline
    val spark = Harness.session()
    val whatIf = try WhatIfTrace.build(spark, log, conf) finally spark.stop()
    val recorded = AroTrace.build(log)
    val info0 = whatIf("what_if").asInstanceOf[Obj]
    val recuts = info0("recut_stages").asInstanceOf[Seq[Obj]]
    info(s"${recuts.size} of ${stages(recorded).size} stages re-cut, flags " +
      s"${info0("flags").asInstanceOf[Seq[String]].mkString(",")}; tasks recorded " +
      s"${taskCounts(recorded).sum}, what-if ${taskCounts(whatIf).sum}, real ${taskCounts(real).sum}")
    info(f"task time: recorded ${taskSeconds(recorded)}%.1f s, what-if ${taskSeconds(whatIf)}%.1f s, " +
      f"real ${taskSeconds(real)}%.1f s")

    // The simulator replays these after the test, also when it fails.
    AroTrace.write(whatIf, new File(s"target/aro-traces/whatif-$name"))
    AroTrace.write(real, new File(s"target/aro-traces/real-$name"))
    AroTrace.write(recorded, new File("target/aro-traces/baseline"))

    assert(recuts.nonEmpty)
    assert(whatIf("config").asInstanceOf[Map[String, String]].filterKeys(conf.contains) == conf)
    if (exact) assert(taskCounts(whatIf) == taskCounts(real))
  }

  check("advisory-1m", Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1m"), exact = true)
  check("coalesce-off", Map("spark.sql.adaptive.coalescePartitions.enabled" -> "false"),
    exact = true)
  // A different partition count folds the recorded partitions: approximate in bytes, so the
  // coalesced task counts may differ from the real run by a few.
  check("partitions-32", Map("spark.sql.shuffle.partitions" -> "32"), exact = false)

  test("a candidate that changes a join strategy is refused") {
    val spark = Harness.session()
    val e = try intercept[ReplayUnsupportedException] {
      WhatIfTrace.build(spark, baseline, Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"))
    } finally spark.stop()
    assert(e.getMessage.contains("changes shape"))
  }

  test("a query without a captured plan is flagged: its stages stay as recorded") {
    val log = new File(baseline)
    val lines = Files.readAllLines(log.toPath).asScala
    val dropped = lines.lastIndexWhere(_.contains("SparkListenerSQLReplayPlan"))
    val copy = new File(Files.createTempDirectory("replay-uncaptured").toFile, log.getName)
    Files.write(copy.toPath, lines.patch(dropped, Nil, 1).asJava)
    val spark = Harness.session()
    val whatIf = try {
      WhatIfTrace.build(spark, copy.getPath,
        Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1m"))
    } finally spark.stop()
    val flags = whatIf("what_if").asInstanceOf[Obj]("flags").asInstanceOf[Seq[String]]
    assert(flags.contains("UNCAPTURED_EXECUTION"))
  }
}
