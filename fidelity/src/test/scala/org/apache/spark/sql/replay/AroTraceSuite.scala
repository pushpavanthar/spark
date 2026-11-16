package org.apache.spark.sql.replay

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.funsuite.AnyFunSuite

/** The exported simulator trace holds the run the event log recorded. */
class AroTraceSuite extends AnyFunSuite {
  test("an event log exports as a simulator trace with every task and exact stage parents") {
    // Executor settings mean nothing in local mode; the simulator reads the baseline shape there.
    val log = Harness.runWithEventLog(4, "measure", Map("spark.executor.instances" -> "1",
      "spark.executor.cores" -> "4", "spark.executor.memory" -> "4g"))
    val model = EventLogReader.read(log)
    // A fixed place, so the trace can be handed to the simulator after the test.
    val path = AroTrace.export(log, new File("target/aro-traces").getPath)
    info(s"trace: ${new File(path).getAbsolutePath}")

    val in = new GZIPInputStream(new FileInputStream(path))
    val trace = try new ObjectMapper().readTree(in) finally in.close()
    def list(n: JsonNode): Seq[JsonNode] = n.elements().asScala.toList
    val stages = list(trace.get("stages"))
    val tasks = stages.flatMap(s => list(s.get("tasks")))

    assert(trace.get("schema_version").asInt == 1)
    assert(trace.get("config").get("spark.executor.cores").asText == "4")
    assert(list(trace.get("jobs")).size == model.jobs.size)
    assert(tasks.size == model.jobs.flatMap(_.stages).map(_.tasks.size).sum)
    assert(tasks.map(_.get("duration_ms").asLong).sum ==
      model.jobs.flatMap(_.stages).flatMap(_.tasks).map(_.durationMs).sum)
    assert(tasks.forall(_.get("status").asText == "SUCCESS"))
    assert(list(trace.get("executors")).map(_.get("cores").asInt) == Seq(4))

    // Every stage that reads a shuffle names the stage that wrote it, directly or through the
    // skipped stage standing in for it.
    val same = list(trace.get("skipped_stages"))
      .map(s => s.get("stage_id").asInt -> s.get("same_as").asInt).toMap
    val wrote = stages.filter(_.get("shuffle_write_bytes").asLong > 0).map(_.get("stage_id").asInt)
    val readers = stages.filter(_.get("shuffle_read_bytes").asLong > 0)
    assert(readers.nonEmpty)
    readers.foreach { s =>
      val parents = list(s.get("parents_inferred")).map(_.asInt).map(p => same.getOrElse(p, p))
      assert(parents.nonEmpty && parents.forall(wrote.contains), s"stage ${s.get("stage_id")}")
    }
  }

  test("a compacted event log is refused") {
    val dir = java.nio.file.Files.createTempDirectory("replay-compact").toFile
    val app = new File(dir, "eventlog_v2_app-1")
    app.mkdir()
    new File(app, "appstatus_app-1").createNewFile()
    new File(app, "events_1_app-1.compact").createNewFile()
    val e = intercept[IllegalArgumentException](EventLogReader.read(app.getPath))
    assert(e.getMessage.contains("compacted"))
  }
}
