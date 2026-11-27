package org.apache.spark.sql.replay

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, Exchange, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * What every stage of a recorded run did, next to what it cost: the operators of its plan
 * fragment with the rows they produced, the bytes it read and wrote, and its tasks' time. The
 * rows a rate model is fitted on, and the description a stage of a candidate plan is priced from.
 */
object StageFeatures {
  import WhatIfTrace.{num, successful, Obj}

  /**
   * A stage's plan: `root` down to the exchanges it reads, and the exchange it feeds if any.
   * @param parent position, in the list `fragments` returns, of the fragment that reads it
   */
  case class Fragment(root: SparkPlan, output: Option[Exchange], parent: Int)

  def fragments(plan: SparkPlan): Seq[Fragment] = {
    val out = mutable.Buffer(Fragment(plan, None, -1))
    def below(p: SparkPlan, reader: Int): Unit = p match {
      case s: QueryStageExec => s.plan match {
        case e: Exchange =>
          out += Fragment(e.child, Some(e), reader)
          below(e.child, out.length - 1)
        // A reused exchange: its stage is listed where the exchange is defined.
        case _ =>
      }
      case o => o.children.foreach(below(_, reader))
    }
    below(plan, 0)
    out.toSeq
  }

  /** The operators that run in the stage of `root`: down to, and without, the stages it reads. */
  def local(root: SparkPlan): Seq[SparkPlan] = root match {
    case _: QueryStageExec => Nil
    case o => o +: o.children.flatMap(local)
  }

  /**
   * Quantities of one fragment. Rows are what the recorded run counted for the same logical
   * operator, so a candidate plan that moves an operator to another stage keeps its rows.
   */
  def describe(f: Fragment, rows: Map[String, Long]): Map[String, Any] = {
    val q = mutable.Map[String, Double]().withDefaultValue(0.0)
    val reads = mutable.Buffer[String]()
    def out(p: SparkPlan): Option[Double] = rows.get(Ctx.key(p)).map(_.toDouble)
    def add(name: String, value: Option[Double]): Unit = value match {
      case Some(v) => q(name) += v
      case None => q(name + "_unknown") += 1
    }
    def walk(p: SparkPlan): Unit = p match {
      case s: ShuffleQueryStageExec =>
        val fake = WhatIfTrace.fake(s)
        reads += fake.recordedKey
        q("shuffle_read_bytes") += Option(fake.bytesByPartitionId).map(_.sum.toDouble).getOrElse(0.0)
      case _: QueryStageExec => q("broadcast_reads") += 1
      case r: AQEShuffleReadExec => walk(r.child)
      case o =>
        o match {
          case j: BroadcastHashJoinExec => add("bhj_rows", out(j))
          case j: SortMergeJoinExec => add("smj_rows", out(j))
          case j: ShuffledHashJoinExec => add("shj_rows", out(j))
          case s: SortExec => add("sort_rows", out(s.child))
          case a: BaseAggregateExec => add("agg_rows", out(a.child))
          case l if l.children.isEmpty => add("scan_rows", out(l))
          case _ =>
        }
        o.children.foreach(walk)
    }
    walk(f.root)
    val written = f.output.collect { case e: FakeShuffleExchangeExec =>
      Option(e.bytesByPartitionId).map(_.sum.toDouble).getOrElse(0.0)
    }
    q.toMap ++ Map("reads" -> reads.toSeq,
      "output" -> f.output.map {
        case _: ShuffleExchangeLike => "shuffle"
        case _: BroadcastExchangeLike => "broadcast"
        case o => o.nodeName
      }.getOrElse("result"),
      "shuffle_write_bytes" -> written.getOrElse(0.0),
      "write_key" -> f.output.collect { case e: FakeShuffleExchangeExec => e.recordedKey })
  }

  /** One row per stage of the run that a fragment of a reproduced query is matched to. */
  def extract(spark: SparkSession, logPath: String): Seq[Map[String, Any]] = {
    val captured = new WhatIfTrace.Captured
    val trace = AroTrace.build(logPath, listeners = Seq(captured))
    val conf = trace("config").asInstanceOf[Map[String, String]]
    val stages = trace("stages").asInstanceOf[Seq[Obj]].map(s => s("stage_id") -> s).toMap[Any, Obj]
    val sameAs = trace("skipped_stages").asInstanceOf[Seq[Obj]]
      .flatMap(s => s("same_as").asInstanceOf[Option[Int]].map(s("stage_id") -> _)).toMap[Any, Int]
    def parents(s: Obj): Set[Int] =
      s("parents_inferred").asInstanceOf[Seq[Int]].map(p => sameAs.getOrElse(p, p)).toSet

    captured.plans.sortBy(_.executionId).flatMap { plan =>
      val shuffles = captured.shuffles.filter(_.executionId == plan.executionId).toSeq
      val cap = QueryCapture(plan, shuffles)
      val session = spark.newSession()
      (conf ++ captured.sessionConf.getOrElse(plan.executionId, Map())).foreach { case (k, v) =>
        if (k.startsWith("spark.sql.") && !SQLConf.isStaticConfigKey(k)) session.conf.set(k, v)
      }
      val (was, _) = Replay.replay(session, cap)
      val rows = plan.rowKeys.zip(plan.rowValues).toMap
      def writer(key: String): Option[Int] =
        shuffles.find(_.key == key).map(_.writerStageId).filter(_ >= 0)
      if (!Replay.reproduces(was, cap)) Nil
      else fragments(was).flatMap { f =>
        val d = describe(f, rows)
        val stage = d("write_key").asInstanceOf[Option[String]].flatMap(writer).orElse {
          val writers = d("reads").asInstanceOf[Seq[String]].flatMap(writer).toSet
          if (writers.isEmpty) None
          else stages.values.filter(s => parents(s) == writers).map(_("stage_id").asInstanceOf[Int])
            .toSeq match { case Seq(one) => Some(one); case _ => None }
        }
        stage.flatMap(stages.get).map { s =>
          val tasks = successful(s)
          def sum(k: String): Double = tasks.map(num(_, k)).sum
          (d - "write_key" - "reads") ++ Map("execution" -> plan.executionId,
            "stage_id" -> s("stage_id"), "tasks" -> tasks.size, "duration_ms" -> sum("duration_ms"),
            "cpu_ms" -> sum("cpu_ms"), "run_ms" -> sum("run_ms"), "gc_ms" -> sum("gc_ms"),
            "write_ms" -> sum("shuffle_write_ms"), "input_bytes" -> sum("input_bytes"),
            "recorded_read_bytes" -> sum("shuffle_read_bytes"),
            "recorded_write_bytes" -> sum("shuffle_write_bytes"), "spill_mem" -> sum("spill_mem"))
        }
      }
    }
  }
}
