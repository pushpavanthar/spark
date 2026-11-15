package org.apache.spark.sql.replay

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.util.zip.GZIPOutputStream

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.status.{AppStatusListener, AppStatusStore, ElementTrackingStore}
import org.apache.spark.status.api.v1
import org.apache.spark.util.kvstore.InMemoryStore

/**
 * Writes one application's event log as a trace of the adaptive-resource-simulator (`aro_sim`,
 * schema 1): the file its `simulate` and `check` commands load in place of a History Server
 * extract. Field names and units follow `aro_sim/trace.py`.
 *
 * The values come from the same status store the History Server serves its REST API from, so a
 * trace built here equals one extracted over REST, except that stage parents are the exact ones
 * of the event log instead of inferred ones.
 */
object AroTrace {
  private val SchemaVersion = 1

  def export(logPath: String, outDir: String,
      hadoopConf: Configuration = new Configuration()): String = {
    val trace = build(logPath, hadoopConf)
    val dir = new File(outDir, trace("app").asInstanceOf[Map[String, Any]]("id").toString)
    dir.mkdirs()
    val file = new File(dir, "trace.json.gz")
    val out = new GZIPOutputStream(new FileOutputStream(file))
    try new ObjectMapper().registerModule(DefaultScalaModule).writeValue(out, trace)
    finally out.close()
    file.getPath
  }

  def build(logPath: String, hadoopConf: Configuration = new Configuration()): Map[String, Any] = {
    // A history replay trims the store to what the UI shows; a trace needs every task.
    val conf = new SparkConf(false)
    Seq("Jobs", "Stages", "Tasks", "DeadExecutors").foreach { what =>
      conf.set(s"spark.ui.retained$what", Int.MaxValue.toString)
    }
    val kv = new ElementTrackingStore(new InMemoryStore(), conf)
    val parents = mutable.Map[Int, Seq[Int]]()
    val parentListener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit =
        e.stageInfos.foreach(s => parents(s.stageId) = s.parentIds)
    }
    EventLogReader.replay(logPath, hadoopConf,
      Seq(new AppStatusListener(kv, conf, live = false), parentListener))
    val store = new AppStatusStore(kv)
    try trace(store, parents.toMap) finally store.close()
  }

  private def trace(store: AppStatusStore, parents: Map[Int, Seq[Int]]): Map[String, Any] = {
    val app = store.applicationInfo()
    val attempt = app.attempts.last
    val t0 = attempt.startTime.getTime
    def at(d: Date): Double = (d.getTime - t0) / 1000.0
    def firstLine(s: Option[String], max: Int): Option[String] =
      s.map(_.trim.split("\n").head.take(max)).filter(_.nonEmpty)

    def task(t: v1.TaskData): Map[String, Any] = {
      val m = t.taskMetrics
      def metric(f: v1.TaskMetrics => Long): Long = m.map(f).getOrElse(0L)
      Map(
        "index" -> t.index, "attempt" -> t.attempt, "executor" -> t.executorId, "host" -> t.host,
        "status" -> t.status, "launch_s" -> at(t.launchTime),
        "duration_ms" -> t.duration.getOrElse(0L),
        "run_ms" -> metric(_.executorRunTime),
        "cpu_ms" -> metric(_.executorCpuTime) / 1e6,
        "deser_ms" -> metric(_.executorDeserializeTime),
        "gc_ms" -> metric(_.jvmGcTime),
        "sched_delay_ms" -> t.schedulerDelay,
        "peak_exec_mem" -> metric(_.peakExecutionMemory),
        "spill_mem" -> metric(_.memoryBytesSpilled),
        "spill_disk" -> metric(_.diskBytesSpilled),
        "input_bytes" -> metric(_.inputMetrics.bytesRead),
        "input_records" -> metric(_.inputMetrics.recordsRead),
        "output_bytes" -> metric(_.outputMetrics.bytesWritten),
        "output_records" -> metric(_.outputMetrics.recordsWritten),
        "shuffle_read_bytes" ->
          metric(x => x.shuffleReadMetrics.remoteBytesRead + x.shuffleReadMetrics.localBytesRead),
        "shuffle_read_remote_bytes" -> metric(_.shuffleReadMetrics.remoteBytesRead),
        "shuffle_fetch_wait_ms" -> metric(_.shuffleReadMetrics.fetchWaitTime),
        "shuffle_write_bytes" -> metric(_.shuffleWriteMetrics.bytesWritten),
        "shuffle_write_ms" -> metric(_.shuffleWriteMetrics.writeTime) / 1e6,
        "locality" -> t.taskLocality, "speculative" -> t.speculative,
        "error" -> firstLine(t.errorMessage, 300))
    }

    def executor(e: v1.ExecutorSummary): Map[String, Any] = {
      def peak(name: String): Long = e.peakMemoryMetrics.map(_.getMetricValue(name)).getOrElse(0L)
      Map(
        "id" -> e.id, "host" -> e.hostPort.split(":").head, "cores" -> e.totalCores,
        "add_s" -> at(e.addTime), "remove_s" -> e.removeTime.map(at),
        "remove_reason" -> firstLine(e.removeReason, 200),
        "max_memory" -> e.maxMemory,
        "peak_jvm_heap" -> peak("JVMHeapMemory"), "peak_jvm_offheap" -> peak("JVMOffHeapMemory"),
        "peak_onheap_storage" -> peak("OnHeapStorageMemory"),
        "peak_offheap_storage" -> peak("OffHeapStorageMemory"),
        "peak_onheap_execution" -> peak("OnHeapExecutionMemory"),
        "peak_offheap_execution" -> peak("OffHeapExecutionMemory"),
        "disk_used" -> e.diskUsed, "memory_used" -> e.memoryUsed,
        "completed_tasks" -> e.completedTasks, "failed_tasks" -> e.failedTasks)
    }

    val all = store.stageList(null)
    val (skipped, ran) = all.filter(_.status != v1.StageStatus.PENDING)
      .partition(_.status == v1.StageStatus.SKIPPED)
    val stages = ran.sortBy(s => (s.stageId, s.attemptId)).map { s =>
      Map(
        "stage_id" -> s.stageId, "attempt" -> s.attemptId, "status" -> s.status.name,
        "name" -> s.name,
        "submit_s" -> s.submissionTime.map(at), "complete_s" -> s.completionTime.map(at),
        "num_tasks" -> s.numTasks, "num_failed_tasks" -> s.numFailedTasks,
        "input_bytes" -> s.inputBytes, "output_bytes" -> s.outputBytes,
        "shuffle_read_bytes" -> s.shuffleReadBytes, "shuffle_write_bytes" -> s.shuffleWriteBytes,
        "spill_mem" -> s.memoryBytesSpilled, "spill_disk" -> s.diskBytesSpilled,
        "failure_reason" -> firstLine(s.failureReason, 300),
        "rdd_ids" -> s.rddIds.sorted,
        // The simulator's name for the field; these are the event log's own parent ids.
        "parents_inferred" -> parents.getOrElse(s.stageId, Nil),
        "tasks" -> store.taskList(s.stageId, s.attemptId, Int.MaxValue).map(task))
    }
    // A skipped stage is a shuffle reused from an earlier stage over the same RDDs; the simulator
    // follows the link when a lost executor's output has to be recomputed.
    val ranByRdds = ran.sortBy(_.stageId).reverse.map(s => s.rddIds.sorted -> s.stageId).toMap
    val executors = store.executorList(false)
    val (driver, workers) = executors.partition(_.id == "driver")

    Map(
      "schema_version" -> SchemaVersion,
      "app" -> Map("id" -> app.id, "name" -> app.name, "start" -> gmt(attempt.startTime),
        "end" -> gmt(attempt.endTime), "duration_s" -> attempt.duration / 1000.0,
        "completed" -> attempt.completed, "spark_version" -> attempt.appSparkVersion),
      "config" -> store.environmentInfo().sparkProperties.toMap,
      "jobs" -> store.jobsList(null).sortBy(_.jobId).map { j =>
        Map("job_id" -> j.jobId, "name" -> j.name.take(120), "status" -> j.status.name,
          "stage_ids" -> j.stageIds,
          "submit_s" -> j.submissionTime.map(at), "complete_s" -> j.completionTime.map(at))
      },
      "stages" -> stages,
      "skipped_stages" -> skipped.map { s =>
        Map("stage_id" -> s.stageId, "name" -> s.name, "num_tasks" -> s.numTasks,
          "same_as" -> ranByRdds.get(s.rddIds.sorted))
      },
      // In local mode the driver runs the tasks and is the only executor there is.
      "executors" -> (if (workers.nonEmpty) workers else driver).map(executor),
      "driver" -> driver.headOption.map(executor))
  }

  /** The History Server's timestamp format, which the simulator parses. */
  private def gmt(d: Date): String = {
    val f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'")
    f.setTimeZone(TimeZone.getTimeZone("GMT"))
    f.format(d)
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "usage: AroTrace <event-log> <out-dir>")
    println(export(args(0), args(1)))
  }
}
