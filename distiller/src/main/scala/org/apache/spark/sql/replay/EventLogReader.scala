package org.apache.spark.sql.replay

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.scheduler._

case class TaskRun(launchTime: Long, finishTime: Long) {
  def durationMs: Long = finishTime - launchTime
}
case class StageRun(stageId: Int, parentIds: Seq[Int], tasks: Seq[TaskRun])
case class JobRun(jobId: Int, group: Option[String], submitTime: Long, endTime: Long,
    stages: Seq[StageRun])
case class ExecutorRun(id: String, cores: Int, addTime: Long, removeTime: Option[Long])

/** What one application did, as far as scheduling is concerned. */
case class RunModel(jobs: Seq[JobRun], executors: Seq[ExecutorRun]) {
  def spanMs: Long = if (jobs.isEmpty) 0 else jobs.map(_.endTime).max - jobs.map(_.submitTime).min
  def onlyGroup(group: String): RunModel = copy(jobs = jobs.filter(_.group.contains(group)))
}

/** Reads the jobs, stages, tasks and executors of one application from its event log. */
object EventLogReader {
  def read(logPath: String, hadoopConf: Configuration = new Configuration()): RunModel = {
    val collector = new Collector
    replay(logPath, hadoopConf, Seq(collector))
    collector.model
  }

  /** Feeds every event of the log to the listeners, in order. */
  def replay(logPath: String, hadoopConf: Configuration,
      listeners: Seq[SparkListenerInterface]): Unit = {
    val path = new Path(logPath)
    val fs = path.getFileSystem(hadoopConf)
    val reader = EventLogFileReader(fs, path)
      .getOrElse(throw new IllegalArgumentException(s"not an event log: $logPath"))
    val files = reader.listEventLogFiles.map(_.getPath)
    // Compaction drops the task events of finished jobs; a timeline built from it would be wrong.
    files.find(EventLogFileWriter.isCompacted).foreach { f =>
      throw new IllegalArgumentException(s"compacted event log is not usable: $f")
    }
    val bus = new ReplayListenerBus()
    listeners.foreach(bus.addListener)
    files.foreach { f =>
      val in = EventLogFileReader.openEventLog(f, fs)
      try bus.replay(in, f.getName) finally in.close()
    }
  }

  private class Collector extends SparkListener {
    private case class OpenJob(jobId: Int, group: Option[String], submitTime: Long,
        stages: Seq[StageInfo])
    private val open = mutable.Map[Int, OpenJob]()
    private val tasks = mutable.Map[Int, mutable.Buffer[TaskRun]]()
    private val jobs = mutable.Buffer[JobRun]()
    private val executors = mutable.LinkedHashMap[String, ExecutorRun]()

    override def onExecutorAdded(e: SparkListenerExecutorAdded): Unit =
      executors(e.executorId) = ExecutorRun(e.executorId, e.executorInfo.totalCores, e.time, None)

    override def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit =
      executors.get(e.executorId).foreach { x =>
        executors(e.executorId) = x.copy(removeTime = Some(e.time))
      }

    override def onJobStart(e: SparkListenerJobStart): Unit = {
      val group = Option(e.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id")))
      open(e.jobId) = OpenJob(e.jobId, group, e.time, e.stageInfos)
    }

    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = if (e.taskInfo != null) {
      tasks.getOrElseUpdate(e.stageId, mutable.Buffer()) +=
        TaskRun(e.taskInfo.launchTime, e.taskInfo.finishTime)
    }

    override def onJobEnd(e: SparkListenerJobEnd): Unit = open.remove(e.jobId).foreach { j =>
      // Stages listed by the job but never run were skipped: their shuffle output already existed.
      val ran = j.stages.flatMap { s =>
        tasks.remove(s.stageId).map(t => StageRun(s.stageId, s.parentIds, t.sortBy(_.launchTime).toList))
      }
      jobs += JobRun(j.jobId, j.group, j.submitTime, e.time, ran)
    }

    def model: RunModel = RunModel(jobs.sortBy(_.submitTime).toList, executors.values.toList)
  }
}
