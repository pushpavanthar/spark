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

  // A re-cut task that holds more data than its memory grant spills the excess. What a spilled
  // GB costs in task time is the simulator's fitted constant (aro_sim memory.py).
  private val SpillMsPerByte = 3.63 * 1000 / 1e9
  private val ReservedBytes = 300L << 20

  private type Obj = Map[String, Any]

  /** Execution memory one task can count on, in bytes: the pool of an executor over its cores. */
  private def memoryGrant(conf: Map[String, String]): Double = {
    def bytes(key: String, default: String): Double =
      JavaUtils.byteStringAsBytes(conf.getOrElse(key, default)).toDouble
    val cores = conf.getOrElse("spark.executor.cores", "1").toInt
    if (conf.get("spark.memory.offHeap.enabled").contains("true")) {
      bytes("spark.memory.offHeap.size", "0") / cores
    } else {
      (bytes("spark.executor.memory", "1g") - ReservedBytes) *
        conf.getOrElse("spark.memory.fraction", "0.6").toDouble / cores
    }
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

  /**
   * The stage with one task per entry of `sizes`. The work its recorded tasks did is shared out
   * by bytes. Spill is the recorded spill plus the change the memory rule expects: working memory
   * per byte is what the stage's tasks that did not spill needed, and a task spills what it
   * holds beyond its grant.
   */
  private def recut(stage: Obj, sizes: Seq[Long], grant: Double): Obj = {
    val src = stage("tasks").asInstanceOf[Seq[Obj]].filter(_("status") == "SUCCESS")
      .sortBy(num(_, "index"))
    val total = sizes.sum.toDouble
    val share = sizes.map(s => if (total > 0) s / total else 1.0 / sizes.length)
    val fetch = src.map(t => math.min(num(t, "duration_ms"), num(t, "shuffle_fetch_wait_ms"))).sum
    val newFetch = fetch * math.pow(sizes.length.toDouble / src.length, FetchPartitionExp)

    val read = src.map(num(_, "shuffle_read_bytes"))
    // Working memory per byte read. A task that spilled shows it directly: what it held plus
    // what it spilled. Small tasks overstate it (buffers have a minimum size), so tasks that
    // did not spill are only used when none did.
    val (wet, calm) = src.partition(num(_, "spill_mem") > 0)
    val basis = if (wet.nonEmpty) wet else calm
    val basisRead = basis.map(num(_, "shuffle_read_bytes")).sum
    val memoryPerByte =
      if (basisRead > 0) basis.map(t => num(t, "peak_exec_mem") + num(t, "spill_mem")).sum / basisRead
      else 0.0
    def expectedSpill(bytes: Double): Double = math.max(0.0, memoryPerByte * bytes - grant)
    val newRead = share.map(_ * read.sum)
    val expected = newRead.map(expectedSpill)
    val spilled = src.map(num(_, "spill_mem")).sum
    val spill = math.max(0.0, spilled + expected.sum - read.map(expectedSpill).sum)
    // ponytail: disk bytes per spilled byte as recorded; a stage that never spilled gets none,
    // so node disk is underestimated for it. Fit a ratio if the disk model starts to matter.
    val diskPerSpilled = if (spilled > 0) src.map(num(_, "spill_disk")).sum / spilled else 0.0
    val work = math.max(0.0,
      src.map(num(_, "duration_ms")).sum - fetch - spilled * SpillMsPerByte)

    val sums = Additive.map(k => k -> src.map(num(_, k)).sum).toMap
    val tasks = share.indices.map { j =>
      val w = share(j)
      val mine = if (expected.sum > 0) spill * expected(j) / expected.sum else spill * w
      // Executor, host and launch time of the recorded task at the same relative position.
      src(j * src.length / sizes.length) ++ sums.map { case (k, v) => k -> v * w } ++ Map(
        "index" -> j, "attempt" -> 0,
        "duration_ms" -> ((work + newFetch) * w + mine * SpillMsPerByte),
        "shuffle_fetch_wait_ms" -> newFetch * w,
        "peak_exec_mem" -> math.min(memoryPerByte * newRead(j), grant), "spill_mem" -> mine,
        "spill_disk" -> mine * diskPerSpilled)
    }
    stage ++ Map("tasks" -> tasks, "num_tasks" -> sizes.length)
  }

  /**
   * @param spark a session with [[Replay.install]]; nothing is executed on it
   * @param conf the candidate configuration, on top of what each query ran with
   */
  def build(spark: SparkSession, logPath: String, conf: Map[String, String]): Map[String, Any] = {
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

    val grant = memoryGrant(recordedConf)
    val flags = mutable.LinkedHashSet[String]()
    // A query whose capture failed keeps its recorded stages, whatever the candidate changes.
    if ((captured.sessionConf.keySet -- captured.plans.map(_.executionId)).nonEmpty) {
      flags += "UNCAPTURED_EXECUTION"
    }
    val recuts = mutable.Buffer[Obj]()
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
          stages(key) = recut(stages(key), cut.sizes.get, grant)
          recuts += Map("stage_id" -> key._1, "tasks_before" -> before,
            "tasks_after" -> cut.sizes.get.length)
        case _ =>
      }
    }
    trace ++ Map(
      "stages" -> stages.values.toSeq,
      "config" -> (recordedConf ++ conf),
      "what_if" -> Map("conf" -> conf, "flags" -> flags.toSeq, "recut_stages" -> recuts.toSeq))
  }

  /** Writes `outDir/name/trace.json.gz` and returns its path. */
  def export(spark: SparkSession, logPath: String, conf: Map[String, String], outDir: String,
      name: String): String =
    AroTrace.write(build(spark, logPath, conf), new File(outDir, name))
}
