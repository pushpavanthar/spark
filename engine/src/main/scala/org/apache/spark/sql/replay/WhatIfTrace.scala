package org.apache.spark.sql.replay

import java.io.File

import scala.collection.mutable

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.internal.SQLConf

/**
 * The simulator trace of a recorded run, rewritten to the run it would have been under other SQL
 * configuration: every query is replayed under its recorded and under the candidate configuration,
 * and each stage whose shuffle read is cut differently gets the candidate's tasks.
 *
 * Only repartitioning is exported. A candidate that changes the shape of a plan (a join strategy,
 * an exchange) is refused: its stages have no recorded counterpart to take task costs from.
 */
object WhatIfTrace {
  // A stage's total fetch wait grows with its task count to this power. The simulator's own
  // constant (aro_sim cost.py, FETCH_PARTITION_EXP), so both tools price a re-cut the same way.
  private val FetchPartitionExp = 0.89

  // Task metrics that add up over a stage and follow the data: a task gets its share of bytes.
  private val Additive = Seq("run_ms", "cpu_ms", "gc_ms", "input_bytes", "input_records", "output_bytes", "output_records", "shuffle_read_bytes",
    "shuffle_read_remote_bytes", "shuffle_write_bytes", "shuffle_write_ms")

  // What a spilled GB costs in CPU when the run itself has no spilled task to measure it on:
  // the simulator's fitted constant (aro_sim memory.py).
  private val DefaultSpillCpuMsPerByte = 3.63 * 1000 / 1e9
  // ponytail: spill files on disk per byte spilled from memory, when the run never spilled. A
  // quarter is what the cluster validation runs showed; it follows the data's compressibility.
  private val DefaultDiskPerSpilled = 0.25
  private val ReservedBytes = 300L << 20

  private type Obj = Map[String, Any]

  /**
   * How the recorded run spilled, measured on its own tasks so that it holds for this job on
   * this hardware.
   *
   * @param pool execution memory of one executor, in bytes
   * @param cpuMsPerByte CPU a task spends per byte it spills from memory
   * @param diskPerSpilled bytes written to disk per byte spilled from memory
   * @param diskBytesPerMs what the disks behind the executors move when spilling tasks wait on
   *        them; None when no recorded task waited, so the limit is unknown
   */
  private case class SpillModel(pool: Double, cores: Int, executors: Int, cpuMsPerByte: Double,
      diskPerSpilled: Double, diskBytesPerMs: Option[Double]) {
    def slots: Int = cores * executors

    /**
     * Memory each task can hold. Tasks of an executor share its pool equally, but a task far
     * larger than the rest runs on after they finished and then has the pool to itself: only
     * tasks at least half its size count as sharing with it.
     */
    def grants(read: Seq[Double]): Seq[Double] = read.map { mine =>
      val peers = math.ceil(read.count(_ >= mine / 2).toDouble / executors).toInt
      pool / math.min(math.max(peers, 1), cores)
    }

    /** How many tasks of the stage use the disk while this one does: see `grants`. */
    def spillPeers(spills: Seq[Double], mine: Double): Int =
      math.min(math.max(spills.count(s => s > 0 && s >= mine / 2), 1), slots)

    /**
     * Time each task waits for the disk, given what each spills from memory. A spill is written
     * once and read back once, and the disk is shared with the tasks spilling alongside.
     */
    def diskWaitMs(spills: Seq[Double]): Seq[Double] = spills.map { mine =>
      diskBytesPerMs.map(2 * mine * diskPerSpilled / _ * spillPeers(spills, mine)).getOrElse(0.0)
    }
  }

  private def successful(stage: Obj): Seq[Obj] =
    stage("tasks").asInstanceOf[Seq[Obj]].filter(_("status") == "SUCCESS")

  /** The model on the executor shape of another configuration, at the rates measured here. */
  private def reshaped(model: SpillModel, conf: Map[String, String]): SpillModel = {
    def bytes(key: String, default: String): Double =
      JavaUtils.byteStringAsBytes(conf.getOrElse(key, default)).toDouble
    val pool =
      if (conf.get("spark.memory.offHeap.enabled").contains("true")) {
        bytes("spark.memory.offHeap.size", "0")
      } else {
        (bytes("spark.executor.memory", "1g") - ReservedBytes) *
          conf.getOrElse("spark.memory.fraction", "0.6").toDouble
      }
    model.copy(pool = pool, cores = conf.getOrElse("spark.executor.cores", "1").toInt,
      executors = math.max(conf.getOrElse("spark.executor.instances", "1").toInt, 1))
  }

  private def spillModel(conf: Map[String, String], stages: Iterable[Obj]): SpillModel = {
    val shape = reshaped(SpillModel(0, 1, 1, 0, 0, None), conf)
    import shape.{cores, executors, pool}

    // Per stage, tasks that spilled against those that did not: what the first spent beyond the
    // rate of the second, in CPU and in waiting, is what spilling cost them.
    var spilled, disk, extraCpu, extraWait, diskTimesTasks = 0.0
    stages.foreach { stage =>
      val (wet, calm) = successful(stage).filter(num(_, "shuffle_read_bytes") > 0)
        .partition(num(_, "spill_mem") > 0)
      val calmRead = calm.map(num(_, "shuffle_read_bytes")).sum
      if (wet.nonEmpty && calmRead > 0) {
        def wait(t: Obj): Double = num(t, "run_ms") - num(t, "cpu_ms")
        val cpuRate = calm.map(num(_, "cpu_ms")).sum / calmRead
        val waitRate = calm.map(wait).sum / calmRead
        val wetRead = wet.map(num(_, "shuffle_read_bytes")).sum
        spilled += wet.map(num(_, "spill_mem")).sum
        disk += wet.map(num(_, "spill_disk")).sum
        extraCpu += wet.map(num(_, "cpu_ms")).sum - cpuRate * wetRead
        extraWait += wet.map(wait).sum - waitRate * wetRead
        val spills = wet.map(num(_, "spill_mem"))
        diskTimesTasks += wet.map { t =>
          num(t, "spill_disk") * math.min(spills.count(_ >= num(t, "spill_mem") / 2), cores * executors)
        }.sum
      }
    }
    SpillModel(pool, cores, executors,
      if (spilled > 0 && extraCpu > 0) extraCpu / spilled else DefaultSpillCpuMsPerByte,
      if (spilled > 0) disk / spilled else DefaultDiskPerSpilled,
      if (extraWait > 0 && diskTimesTasks > 0) Some(2 * diskTimesTasks / extraWait) else None)
  }

  /**
   * One scheduler stage of a replayed plan.
   * @param reads keys of the shuffles it reads
   * @param sizes bytes of each of its partitions; None when no shuffle read decides them
   */
  private case class Fragment(reads: Seq[String], sizes: Option[Seq[Long]])

  private class Captured extends SparkListener {
    val plans = mutable.Buffer[SparkListenerSQLReplayPlan]()
    val shuffles = mutable.Buffer[SparkListenerSQLShuffleStageStats]()
    val sessionConf = mutable.Map[Long, Map[String, String]]()
    override def onOtherEvent(e: SparkListenerEvent): Unit = e match {
      case p: SparkListenerSQLReplayPlan => plans += p
      case s: SparkListenerSQLShuffleStageStats => shuffles += s
      case s: SparkListenerSQLExecutionStart => sessionConf(s.executionId) = s.modifiedConfigs
      case _ =>
    }
  }

  private def unsupported(what: String): Nothing = throw new ReplayUnsupportedException(what)

  private def fake(s: ShuffleQueryStageExec): FakeShuffleExchangeExec = s.shuffle match {
    case f: FakeShuffleExchangeExec => f
    case o => unsupported(s"not a replayed shuffle: ${o.nodeName}")
  }

  private def specSize(f: FakeShuffleExchangeExec, spec: ShufflePartitionSpec): Long = {
    val bytes = Option(f.bytesByPartitionId).getOrElse(Array.empty[Long])
    val mappers = math.max(f.numMappers, 1)
    spec match {
      case CoalescedPartitionSpec(from, to, size) => size.getOrElse(bytes.slice(from, to).sum)
      case PartialReducerPartitionSpec(_, _, _, size) => size
      // Per-map sizes are recorded for skew candidates only; a local read is priced as if every
      // map task wrote the same amount.
      case PartialMapperPartitionSpec(_, from, to) => bytes.slice(from, to).sum / mappers
      case CoalescedMapperPartitionSpec(from, to, _) => bytes.sum * (to - from) / mappers
    }
  }

  /** Bytes per partition of the stage this node runs in. */
  private def sizes(p: SparkPlan): Option[Seq[Long]] = p match {
    case AQEShuffleReadExec(s: ShuffleQueryStageExec, specs) =>
      Some(specs.map(specSize(fake(s), _)))
    case s: ShuffleQueryStageExec =>
      val f = fake(s)
      Some(Option(f.bytesByPartitionId).map(_.toSeq).getOrElse(Seq.fill(f.numPartitions)(0L)))
    // A broadcast is built by another stage; here it is a value, not partitions.
    case _: QueryStageExec => None
    case u: UnionExec =>
      val children = u.children.map(sizes)
      if (children.forall(_.isEmpty)) None
      else if (children.exists(_.isEmpty)) unsupported("a stage unions a scan with a shuffle read")
      else Some(children.flatten.flatten)
    case o =>
      val children = o.children.flatMap(sizes(_))
      if (children.isEmpty) None
      else if (children.map(_.length).distinct.size > 1) {
        unsupported(s"${o.nodeName} reads shuffles with different partition counts")
      } else Some(children.transpose.map(_.sum))
  }

  private def reads(p: SparkPlan): Seq[String] = p match {
    case s: ShuffleQueryStageExec => Seq(fake(s).recordedKey)
    case _: QueryStageExec => Nil
    case o => o.children.flatMap(reads)
  }

  /** Root of the plan each exchange's stage runs. Subqueries are not followed. */
  private def stageRoots(p: SparkPlan): Seq[SparkPlan] = p match {
    case s: QueryStageExec => s.plan match {
      case e: Exchange => e.child +: stageRoots(e.child)
      // A reused exchange: its stage is listed where the exchange is defined.
      case _ => Nil
    }
    case o => o.children.flatMap(stageRoots)
  }

  private def fragments(plan: SparkPlan): Seq[Fragment] =
    (plan +: stageRoots(plan)).map(r => Fragment(reads(r), sizes(r)))

  /** Operators of the plan without the AQE reads: what a repartitioning leaves untouched. */
  private def shape(p: SparkPlan): Seq[String] = p match {
    case r: AQEShuffleReadExec => shape(r.child)
    case s: QueryStageExec => shape(s.plan)
    case o => o.getClass.getSimpleName +: o.children.flatMap(shape)
  }

  private def num(o: Obj, key: String): Double = o(key).asInstanceOf[Number].doubleValue

  // What a task held plus what it spilled is what it needed, per byte it read.
  private def memoryPerByte(tasks: Seq[Obj]): Double = {
    val read = tasks.map(num(_, "shuffle_read_bytes")).sum
    if (read > 0) tasks.map(t => num(t, "peak_exec_mem") + num(t, "spill_mem")).sum / read else 0.0
  }

  private def expectedSpill(model: SpillModel, perByte: Double, reads: Seq[Double]): Seq[Double] =
    reads.zip(model.grants(reads)).map { case (r, g) => math.max(0.0, perByte * r - g) }

  // Time a task waited without using CPU, beyond `explained`. Measured on a cluster, it is time
  // spent waiting for a device the running tasks share, and so scales with how many run at once.
  private def idleMs(t: Obj, explained: Double): Double =
    math.max(0.0, num(t, "run_ms") - num(t, "cpu_ms") - num(t, "gc_ms") - explained)

  /**
   * The stage with one task per entry of `sizes`, on the executor shape of `model`. The work its
   * recorded tasks did is shared out by bytes. Spill is the recorded spill plus the change the
   * memory rule expects: a task holds the stage's working memory per byte times what it reads,
   * and spills what exceeds its grant.
   */
  private def recut(stage: Obj, sizes: Seq[Long], old: SpillModel, model: SpillModel): Obj = {
    val src = successful(stage).sortBy(num(_, "index"))
    val total = sizes.sum.toDouble
    val share = sizes.map(s => if (total > 0) s / total else 1.0 / sizes.length)
    val fetch = src.map(t => math.min(num(t, "duration_ms"), num(t, "shuffle_fetch_wait_ms"))).sum
    val newFetch = fetch * math.pow(sizes.length.toDouble / src.length, FetchPartitionExp)

    val read = src.map(num(_, "shuffle_read_bytes"))
    val perByte = memoryPerByte(src)
    val newRead = share.map(_ * read.sum)
    val expected = expectedSpill(model, perByte, newRead)
    val recorded = src.map(num(_, "spill_mem"))
    val spill = math.max(0.0, recorded.sum + expected.sum - expectedSpill(old, perByte, read).sum)
    val mine = share.indices.map { j =>
      if (expected.sum > 0) spill * expected(j) / expected.sum else spill * share(j)
    }
    val diskWait = model.diskWaitMs(mine)
    val oldWait = old.diskWaitMs(recorded)
    val idle = math.max(0.0, src.indices.map(i => idleMs(src(i), oldWait(i))).sum - fetch)
    // Take out what the recorded tasks spent spilling; the new tasks get their own.
    val work = math.max(0.0, src.map(num(_, "duration_ms")).sum - fetch -
      recorded.sum * old.cpuMsPerByte - oldWait.sum - idle)
    val newIdle = idle * model.slots / old.slots

    val grants = model.grants(newRead)
    val sums = Additive.map(k => k -> src.map(num(_, k)).sum).toMap
    val tasks = share.indices.map { j =>
      val w = share(j)
      val spilling = mine(j) * model.cpuMsPerByte + diskWait(j)
      // Executor, host and launch time of the recorded task at the same relative position.
      src(j * src.length / sizes.length) ++ sums.map { case (k, v) => k -> v * w } ++ Map(
        "index" -> j, "attempt" -> 0,
        "duration_ms" -> ((work + newIdle + newFetch) * w + spilling),
        "shuffle_fetch_wait_ms" -> newFetch * w,
        "peak_exec_mem" -> math.min(perByte * newRead(j), grants(j)),
        "spill_mem" -> mine(j), "spill_disk" -> mine(j) * model.diskPerSpilled)
    }
    stage ++ Map("tasks" -> tasks, "num_tasks" -> sizes.length)
  }

  /** The stage's own tasks on the executor shape of `model`: other memory, other concurrency. */
  private def reshape(stage: Obj, old: SpillModel, model: SpillModel): Obj = {
    val src = successful(stage)
    val read = src.map(num(_, "shuffle_read_bytes"))
    val perByte = memoryPerByte(src)
    val recorded = src.map(num(_, "spill_mem"))
    val before = expectedSpill(old, perByte, read)
    val after = expectedSpill(model, perByte, read)
    val spills = recorded.indices.map(i => math.max(0.0, recorded(i) + after(i) - before(i)))
    val oldWait = old.diskWaitMs(recorded)
    val newWait = model.diskWaitMs(spills)
    val grants = model.grants(read)
    val reshaped = src.indices.map { i =>
      val t = src(i)
      val idle = idleMs(t, oldWait(i))
      val rest = math.max(0.0,
        num(t, "duration_ms") - recorded(i) * old.cpuMsPerByte - oldWait(i) - idle)
      (t("index"), t("attempt")) -> (t ++ Map(
        "duration_ms" ->
          (rest + idle * model.slots / old.slots + spills(i) * model.cpuMsPerByte + newWait(i)),
        "peak_exec_mem" -> math.min(num(t, "peak_exec_mem") + recorded(i), grants(i)),
        "spill_mem" -> spills(i), "spill_disk" -> spills(i) * model.diskPerSpilled))
    }.toMap
    stage ++ Map("tasks" -> stage("tasks").asInstanceOf[Seq[Obj]]
      .map(t => reshaped.getOrElse((t("index"), t("attempt")), t)))
  }

  /**
   * @param spark a session with [[Replay.install]]; nothing is executed on it
   * @param conf the candidate configuration, on top of what each query ran with
   * @param repeats event logs of other runs of the job at the recorded configuration; how much
   *        they differ from each other is how much a real run of the candidate may differ too
   */
  def build(spark: SparkSession, logPath: String, conf: Map[String, String],
      repeats: Seq[String] = Nil): Map[String, Any] = {
    val captured = new Captured
    val trace = AroTrace.build(logPath, listeners = Seq(captured))
    val recordedConf = trace("config").asInstanceOf[Map[String, String]]
    val stages = mutable.LinkedHashMap(trace("stages").asInstanceOf[Seq[Obj]]
      .map(s => (s("stage_id"), s("attempt")) -> s): _*)
    val sameAs = trace("skipped_stages").asInstanceOf[Seq[Obj]]
      .flatMap(s => s("same_as").asInstanceOf[Option[Int]].map(s("stage_id") -> _)).toMap[Any, Int]
    // A stage is found by the stages that wrote what it reads. A parent the scheduler skipped
    // stands for the stage that really wrote the shuffle.
    def parents(s: Obj): Set[Int] =
      s("parents_inferred").asInstanceOf[Seq[Int]].map(p => sameAs.getOrElse(p, p)).toSet

    val recordedModel = spillModel(recordedConf, stages.values)
    val model = reshaped(recordedModel, recordedConf ++ conf)
    val recutKeys = mutable.Set[(Any, Any)]()
    val flags = mutable.LinkedHashSet[String]()
    // A query whose capture failed keeps its recorded stages, whatever the candidate changes.
    val uncaptured = (captured.sessionConf.keySet -- captured.plans.map(_.executionId)).toSeq.sorted
    if (uncaptured.nonEmpty) flags += "UNCAPTURED_EXECUTION"
    val recuts = mutable.Buffer[Obj]()
    val mismatched = mutable.Buffer[Long]()
    captured.plans.sortBy(_.executionId).foreach { plan =>
      val id = plan.executionId
      val shuffles = captured.shuffles.filter(_.executionId == id).toSeq
      val ranWith = recordedConf ++ captured.sessionConf.getOrElse(id, Map())
      def replay(c: Map[String, String]): (SparkPlan, Seq[String]) = {
        val session = spark.newSession()
        c.foreach { case (k, v) =>
          if (k.startsWith("spark.sql.") && !SQLConf.isStaticConfigKey(k)) session.conf.set(k, v)
        }
        Replay.replay(session, QueryCapture(plan, shuffles))
      }
      val (was, _) = replay(ranWith)
      if (!Replay.reproduces(was, QueryCapture(plan, shuffles))) {
        flags += "BASELINE_MISMATCH"
        mismatched += id
      }
      val (becomes, raised) = replay(ranWith ++ conf)
      flags ++= raised
      if (shape(was) != shape(becomes)) {
        unsupported(s"execution $id: the plan changes shape under $conf; only a different " +
          "partitioning of the same plan can be exported")
      }
      fragments(was).zip(fragments(becomes)).foreach {
        case (old, cut) if old.sizes.isDefined && old.sizes != cut.sizes =>
          val writers = old.reads.map { key =>
            shuffles.find(_.key == key).map(_.writerStageId).filter(_ >= 0)
              .getOrElse(unsupported(s"execution $id: no recorded stage wrote shuffle $key"))
          }.toSet
          val key = stages.keys.filter(k => parents(stages(k)) == writers).toSeq match {
            case Seq(one) => one
            case found => unsupported(s"execution $id: ${found.size} stages read the shuffles " +
              s"written by stages ${writers.mkString(", ")}, expected one")
          }
          val before = stages(key)("num_tasks")
          // The replay under the recorded configuration must cut the stage the way the run did;
          // otherwise the replay, or the match to this stage, is wrong.
          if (before != old.sizes.get.length) {
            unsupported(s"execution $id: stage ${key._1} ran $before tasks, its replay has " +
              s"${old.sizes.get.length}")
          }
          stages(key) = recut(stages(key), cut.sizes.get, recordedModel, model)
          recutKeys += key
          recuts += Map("stage_id" -> key._1, "tasks_before" -> before,
            "tasks_after" -> cut.sizes.get.length)
        case _ =>
      }
    }
    // Executors with less memory also keep less of the shuffle files in the page cache, and
    // reading them back from disk is not priced: measured 13% too fast at half the memory and
    // 27% at a third, on nodes with a slow disk.
    if (model.pool < recordedModel.pool) flags += "LESS_MEMORY_LOWER_BOUND"
    // Another executor shape touches every stage, not only those the plan cuts differently.
    if (model != recordedModel) {
      stages.keys.filterNot(recutKeys).toSeq.foreach { key =>
        stages(key) = reshape(stages(key), recordedModel, model)
      }
    }
    trace ++ Map(
      "stages" -> stages.values.toSeq,
      "config" -> (recordedConf ++ conf),
      "what_if" -> Map("conf" -> conf, "flags" -> flags.toSeq, "recut_stages" -> recuts.toSeq,
        // What the engine could not reproduce of the run itself: no prediction built on those
        // queries deserves trust, whatever the candidate.
        "calibration" -> Map("executions" -> captured.sessionConf.size,
          "reproduced" -> (captured.plans.size - mismatched.size),
          "mismatched" -> mismatched.toSeq, "uncaptured" -> uncaptured,
          "ok" -> (mismatched.isEmpty && uncaptured.isEmpty)),
        "band" -> band(trace, repeats, recuts.nonEmpty, model != recordedModel),
        "spill_model" -> Map("cpu_s_per_gb" -> model.cpuMsPerByte * 1e6,
          "disk_per_spilled" -> model.diskPerSpilled,
          "disk_mb_per_s" -> model.diskBytesPerMs.map(_ / 1e3))))
  }

  // Largest error of a predicted wall-clock against a real rerun in the cluster validation
  // (README): 10 configuration candidates and 9 executor shapes of one workload on one cluster.
  // A candidate that only cuts memory is excluded; it is flagged as a lower bound instead.
  private val ConfigErrorPct = 16.0
  private val ShapeErrorPct = 17.0

  /**
   * How far a real run of the candidate may be from the prediction, in percent of wall-clock:
   * the run-to-run spread of the job itself, unknown without repeats, and the model's own error.
   */
  private def band(trace: Obj, repeats: Seq[String], recut: Boolean, reshaped: Boolean): Obj = {
    val walls = (trace +: repeats.map(AroTrace.build(_))).map(AroTrace.wallClockSeconds)
    val noise = if (repeats.isEmpty) None else Some((walls.max - walls.min) / walls.min * 100)
    val model = math.max(if (recut) ConfigErrorPct else 0.0, if (reshaped) ShapeErrorPct else 0.0)
    Map("noise_pct" -> noise, "model_pct" -> model,
      "total_pct" -> noise.map(n => math.sqrt(n * n + model * model)))
  }

  /** Writes `outDir/name/trace.json.gz` and returns its path. */
  def export(spark: SparkSession, logPath: String, conf: Map[String, String], outDir: String,
      name: String): String =
    AroTrace.write(build(spark, logPath, conf), new File(outDir, name))
}
