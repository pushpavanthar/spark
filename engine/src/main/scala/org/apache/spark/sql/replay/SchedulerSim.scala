package org.apache.spark.sql.replay

import scala.collection.mutable

/**
 * Replays the recorded jobs of a run on a virtual clock with a different number of task slots.
 *
 * Task durations are the recorded ones. Jobs keep their recorded order: a job is submitted once
 * every job that had finished before it in the real run has finished here, plus the driver time
 * that separated them. FIFO scheduling. Locality, speculation and failures are not modelled.
 */
object SchedulerSim {
  case class Result(spanMs: Long, jobEnd: Map[Int, Long])

  def run(model: RunModel, slots: Int): Result = {
    require(slots > 0, "slots must be positive")
    val jobs = model.jobs
    if (jobs.isEmpty) return Result(0, Map.empty)
    val t0 = jobs.map(_.submitTime).min

    // (time, sequence) ordered events; the sequence keeps equal-time events in insertion order.
    sealed trait Event
    case class Submit(job: JobRun) extends Event
    case class TaskDone(job: JobRun, stage: StageRun) extends Event
    case class JobDone(job: JobRun) extends Event
    var seq = 0L
    val events = mutable.PriorityQueue[(Long, Long, Event)]()(
      Ordering.by[(Long, Long, Event), (Long, Long)](e => (e._1, e._2)).reverse)
    def at(time: Long, e: Event): Unit = { seq += 1; events.enqueue((time, seq, e)) }

    // Recorded predecessors and the driver time between the last of them and this job.
    val preds: Map[Int, Seq[JobRun]] =
      jobs.map(j => j.jobId -> jobs.filter(k => k.jobId != j.jobId && k.endTime <= j.submitTime)).toMap
    def gap(j: JobRun): Long =
      j.submitTime - (if (preds(j.jobId).isEmpty) t0 else preds(j.jobId).map(_.endTime).max)

    val jobEnd = mutable.Map[Int, Long]()
    val submitted = mutable.Set[Int]()
    def submitReady(): Unit = jobs.foreach { j =>
      if (!submitted(j.jobId) && preds(j.jobId).forall(p => jobEnd.contains(p.jobId))) {
        submitted += j.jobId
        val after = if (preds(j.jobId).isEmpty) 0L else preds(j.jobId).map(p => jobEnd(p.jobId)).max
        at(after + gap(j), Submit(j))
      }
    }

    // Pending tasks, earliest job then earliest stage first.
    val pending = mutable.PriorityQueue[(Int, Int, Long, JobRun, StageRun)]()(
      Ordering.by[(Int, Int, Long, JobRun, StageRun), (Int, Int)](p => (p._1, p._2)).reverse)
    val remaining = mutable.Map[(Int, Int), Int]()
    val stagesLeft = mutable.Map[Int, Int]()
    val doneStages = mutable.Map[Int, mutable.Set[Int]]()
    var free = slots

    def release(job: JobRun, stage: StageRun): Unit = {
      remaining((job.jobId, stage.stageId)) = stage.tasks.size
      stage.tasks.foreach(t => pending.enqueue((job.jobId, stage.stageId, t.durationMs, job, stage)))
    }
    def runnable(job: JobRun, stage: StageRun): Boolean = {
      val inJob = job.stages.map(_.stageId).toSet
      stage.parentIds.filter(inJob).forall(doneStages(job.jobId))
    }
    def finish(job: JobRun, now: Long): Unit = {
      // Driver time after the last task of the job (result handling, AQE re-planning).
      val lastTask = (job.stages.flatMap(_.tasks.map(_.finishTime)) :+ job.submitTime).max
      at(now + math.max(0, job.endTime - lastTask), JobDone(job))
    }

    submitReady()
    var now = 0L
    while (events.nonEmpty) {
      val (time, _, event) = events.dequeue()
      now = time
      event match {
        case Submit(job) =>
          doneStages(job.jobId) = mutable.Set()
          stagesLeft(job.jobId) = job.stages.size
          if (job.stages.isEmpty) finish(job, now)
          else job.stages.filter(s => runnable(job, s)).foreach(s => release(job, s))
        case TaskDone(job, stage) =>
          free += 1
          val key = (job.jobId, stage.stageId)
          remaining(key) -= 1
          if (remaining(key) == 0) {
            doneStages(job.jobId) += stage.stageId
            stagesLeft(job.jobId) -= 1
            if (stagesLeft(job.jobId) == 0) finish(job, now)
            else job.stages.filter { s =>
              !doneStages(job.jobId)(s.stageId) && !remaining.contains((job.jobId, s.stageId)) &&
                runnable(job, s)
            }.foreach(s => release(job, s))
          }
        case JobDone(job) =>
          jobEnd(job.jobId) = now
          submitReady()
      }
      while (free > 0 && pending.nonEmpty) {
        val (_, _, duration, job, stage) = pending.dequeue()
        free -= 1
        at(now + duration, TaskDone(job, stage))
      }
    }
    Result(if (jobEnd.isEmpty) 0 else jobEnd.values.max, jobEnd.toMap)
  }
}
