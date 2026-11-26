package org.apache.spark.sql.replay

import java.io.File

import org.scalatest.funsuite.AnyFunSuite

/**
 * The what-if trace against real reruns on a cluster. Needs the event logs of
 * `cluster/replay_validation.py` runs in `$REPLAY_CLUSTER_LOGS/<run>/<event log>`: a `base-1`
 * run with capture and one run for any of the candidates below. Canceled without them. The traces
 * go to `target/aro-traces/<name of that directory>/`.
 */
class ClusterValidationSuite extends AnyFunSuite {
  private type Obj = Map[String, Any]
  private val candidates = Seq(
    "base-2" -> Map[String, String](),
    "adv16m" -> Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "16m"),
    "coalesce-off" -> Map("spark.sql.adaptive.coalescePartitions.enabled" -> "false"),
    "parts200" -> Map("spark.sql.shuffle.partitions" -> "200"),
    "bjt-off" -> Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"),
    "skew-off" -> Map("spark.sql.adaptive.skewJoin.enabled" -> "false"),
    "adv512m" -> Map("spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "512m"),
    "parts50" -> Map("spark.sql.shuffle.partitions" -> "50"))

  private def stages(trace: Obj): Seq[Obj] = trace("stages").asInstanceOf[Seq[Obj]]
  private def taskCounts(trace: Obj): Seq[Int] =
    stages(trace).map(_("tasks").asInstanceOf[Seq[Obj]].size).sorted

  test("what-if traces of a cluster run have the stages and tasks of the real reruns") {
    val root = sys.env.get("REPLAY_CLUSTER_LOGS").map(new File(_))
    assume(root.isDefined, "REPLAY_CLUSTER_LOGS is not set")
    def log(run: String): String = new File(root.get, run).listFiles().head.getPath
    val out = new File("target/aro-traces", root.get.getName)
    AroTrace.write(AroTrace.build(log("base-1")), new File(out, "base-1"))
    val spark = Harness.session()
    try {
      val ran = candidates.filter { case (name, _) => new File(root.get, name).isDirectory }
      val wrong = ran.flatMap { case (name, conf) =>
        val real = AroTrace.build(log(name))
        AroTrace.write(real, new File(out, s"real-$name"))
        try {
          val whatIf = WhatIfTrace.build(spark, log("base-1"), conf)
          AroTrace.write(whatIf, new File(out, s"whatif-$name"))
          val w = whatIf("what_if").asInstanceOf[Obj]
          val same = taskCounts(whatIf) == taskCounts(real)
          info(s"$name: ${w("recut_stages").asInstanceOf[Seq[Obj]].size} stages re-cut, flags " +
            s"[${w("flags").asInstanceOf[Seq[String]].mkString(",")}], tasks what-if " +
            s"${taskCounts(whatIf).sum}, real ${taskCounts(real).sum}, per stage " +
            (if (same) "equal" else "DIFFERENT"))
          // A flagged replay is allowed to be off; an unflagged one is not.
          if (same || w("flags").asInstanceOf[Seq[String]].nonEmpty) None else Some(name)
        } catch {
          case e: ReplayUnsupportedException =>
            info(s"$name: refused - ${e.getMessage.take(200)}")
            None
        }
      }
      assert(wrong.isEmpty)
    } finally spark.stop()
  }
}
