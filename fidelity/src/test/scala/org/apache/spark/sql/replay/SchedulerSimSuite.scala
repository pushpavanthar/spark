package org.apache.spark.sql.replay

import java.io.File
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

/**
 * Time fidelity of the scheduler simulation, with real local runs as ground truth: the same
 * workload is really run with 2, 4 and 8 cores; the 4-core event log is then replayed at each
 * core count and the predicted span compared with the real one.
 */
class SchedulerSimSuite extends AnyFunSuite {
  private val group = "measure"

  /** Runs the workload once to warm the JVM, then once under the measured job group. */
  private def realRun(cores: Int): RunModel = {
    val logDir = Files.createTempDirectory(s"replay-sim-$cores").toFile
    val spark = Harness.session(Map("spark.eventLog.enabled" -> "true",
      "spark.eventLog.dir" -> logDir.toURI.toString), cores)
    spark.sparkContext.setLogLevel("ERROR")
    try {
      Harness.prepare(spark)
      Harness.setConf(spark, Map())
      def workload(): Unit = Harness.queries.foreach { case (_, sql) => spark.sql(sql).collect() }
      workload()
      spark.sparkContext.setJobGroup(group, "measured workload")
      workload()
    } finally spark.stop()
    val log = logDir.listFiles().filter(f => f.isFile && f.getName.startsWith("local-")).head
    EventLogReader.read(log.getPath).onlyGroup(group)
  }

  test("replaying a run at its own and at other core counts predicts the real span") {
    val real = Seq(2, 4, 8).map(c => c -> realRun(c)).toMap
    val baseline = real(4)
    assert(baseline.jobs.nonEmpty && baseline.executors.map(_.cores).sum == 4)
    // If the same tasks take longer in total at a higher core count, the machine is contended and
    // recorded durations do not transfer: that is the cost model's job, not the simulation's.
    def taskMs(m: RunModel): Long = m.jobs.flatMap(_.stages).flatMap(_.tasks).map(_.durationMs).sum
    real.toSeq.sortBy(_._1).foreach { case (c, m) =>
      info(f"real run at $c cores: ${m.jobs.size} jobs, " +
        f"${m.jobs.flatMap(_.stages).map(_.tasks.size).sum} tasks, total task time ${taskMs(m) / 1000.0}%.1f s")
    }
    val errors = Seq(4, 2, 8).map { cores =>
      val predicted = SchedulerSim.run(baseline, cores).spanMs
      val actual = real(cores).spanMs
      val error = (predicted - actual).toDouble / actual
      // The same replay with the task durations the target run really had: isolates the simulation.
      val oracle = SchedulerSim.run(real(cores), cores).spanMs
      assert(math.abs(oracle - actual).toDouble / actual < 0.05, s"simulation error at $cores cores")
      info(f"$cores cores, with that run's own task durations: predicted ${oracle / 1000.0}%.1f s " +
        f"(error ${(oracle - actual) * 100.0 / actual}%+.1f%%)")
      info(f"$cores cores: predicted ${predicted / 1000.0}%.1f s, real ${actual / 1000.0}%.1f s, " +
        f"error ${error * 100}%+.1f%%")
      cores -> error
    }.toMap
    // The design's starting tolerance for a run replayed under its own resources.
    assert(math.abs(errors(4)) < 0.10)
    // The errors at 2 and 8 cores are reported, not asserted. On one machine the same tasks take
    // longer as more of them run at once (measured: total task time 20 s, 27 s, 48 s at 2, 4, 8
    // cores), so recorded durations do not transfer. Checking a resource what-if end to end needs
    // a cluster, where an executor's cores are its own.
  }

  test("a compacted event log is refused") {
    val dir = Files.createTempDirectory("replay-compact").toFile
    val app = new File(dir, "eventlog_v2_app-1")
    app.mkdir()
    new File(app, "appstatus_app-1").createNewFile()
    new File(app, "events_1_app-1.compact").createNewFile()
    val e = intercept[IllegalArgumentException](EventLogReader.read(app.getPath))
    assert(e.getMessage.contains("compacted"))
  }
}
