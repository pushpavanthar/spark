package org.apache.spark.sql.replay

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, SPARK_VERSION, SparkContext, SparkEnv,
  SparkFirehoseListener}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart,
  SparkListenerStageSubmitted}
import org.apache.spark.sql.execution.{CommandResultExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/** Builds the replay events for one finished query from what the driver still holds. */
object ReplayCapture {
  private def clamp(b: BigInt): Long = if (b.isValidLong) b.toLong else Long.MaxValue

  private def allNodes(p: SparkPlan): Seq[SparkPlan] = p +: (p match {
    case s: QueryStageExec => allNodes(s.plan)
    case c: CommandResultExec => allNodes(c.commandPhysicalPlan)
    case o => o.children.flatMap(allNodes)
  })

  /**
   * @param writerStage the stage that wrote a shuffle, by shuffle id
   * @param defaultParallelism of the application while the query ran; 0 when unknown
   */
  def events(executionId: Long, qe: QueryExecution, writerStage: Int => Option[Int],
      defaultParallelism: Int): Seq[SparkListenerEvent] = {
    val logical = SkeletonCodec.unwrapCommand(qe.optimizedPlan)
    val leafOf = logical.collectLeaves().zipWithIndex
      .flatMap { case (l, i) => l.output.map(_.exprId.id -> i) }.toMap
    val encoder = new SkeletonCodec.Encoder
    val root = encoder.plan(logical)

    val top = allNodes(qe.executedPlan)
    val adaptive = top.collectFirst { case a: AdaptiveSparkPlanExec => a }
    // Stages AQE materialized and later eliminated (empty-relation propagation) are no longer in
    // the final plan; the stage cache still holds them, next to stages it created and cancelled.
    val nodes = adaptive.toSeq.flatMap { a =>
      allNodes(a.executedPlan) ++ a.context.stageCache.values.flatMap(allNodes)
    }
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

    val shuffles = nodes.collect {
      case s: ShuffleQueryStageExec
          if s.isMaterialized && s.plan.isInstanceOf[ShuffleExchangeLike] =>
        val e = s.shuffle
        val stats = s.getRuntimeStatistics
        val bytes = s.mapStats.map(_.bytesByPartitionId).getOrElse(Array.empty[Long])
        val (parts, truncated) = Keys.mapSizePartitions(bytes)
        // A lost executor leaves null statuses: the per-map sizes are then incomplete and not
        // recorded, and a replay that needs them raises MISSING_MAP_SIZES.
        val mapSizes = s.mapStats.flatMap(m => tracker.shuffleStatuses.get(m.shuffleId)).flatMap { st =>
          st.withMapStatuses { ms =>
            if (ms.contains(null)) None else Some(parts.map(p => ms.map(_.getSizeForBlock(p))))
          }
        }
        SparkListenerSQLShuffleStageStats(executionId,
          Keys.shufKey(e.child, e.outputPartitioning, leafOf), e.numMappers,
          e.outputPartitioning.numPartitions,
          bytes, stats.rowCount.map(clamp).getOrElse(-1L), clamp(stats.sizeInBytes),
          if (mapSizes.isDefined) parts else Array.empty, mapSizes.getOrElse(Array.empty),
          truncated, s.mapStats.flatMap(m => writerStage(m.shuffleId)).getOrElse(-1))
    }.groupBy(_.key).values.map(_.head).toSeq

    val broadcasts = nodes.collect {
      case b: BroadcastQueryStageExec
          if b.isMaterialized && b.plan.isInstanceOf[BroadcastExchangeLike] =>
        val stats = b.getRuntimeStatistics
        Keys.subtree(b.broadcast.child, leafOf) ->
          ((clamp(stats.sizeInBytes), stats.rowCount.map(clamp).getOrElse(-1L)))
    }.toMap.toSeq
    // Operators planned from one logical node share a key (a filter and the scan under it).
    // The key stands for their subplan, whose output is that of the topmost one: `nodes` lists
    // parents first, and reversed, the topmost is what the map keeps.
    val rows = nodes.flatMap { n =>
      n.metrics.get("numOutputRows").map(v => Keys.subtree(n, leafOf) -> v.value)
    }.reverse.toMap.toSeq

    SparkListenerSQLReplayPlan(executionId, SPARK_VERSION, root, encoder.opaqueNodes,
      rows.map(_._1).toArray, rows.map(_._2).toArray,
      broadcasts.map(_._1).toArray, broadcasts.map(_._2._1).toArray,
      broadcasts.map(_._2._2).toArray, defaultParallelism) +: shuffles
  }
}

/**
 * Builds the replay events of every SQL execution that ends and hands them to `emit`, on the
 * listener thread it is registered on.
 */
class ReplayCapturer(emit: SparkListenerEvent => Unit) extends SparkListener with Logging {
  // The event log does not say which stage wrote which shuffle; only the live stage does.
  private val writerStage = scala.collection.concurrent.TrieMap[Int, Int]()

  override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit =
    e.stageInfo.shuffleDepId.foreach(writerStage.putIfAbsent(_, e.stageInfo.stageId))

  // Read while a job starts: at the end of the last query the context may already be stopping,
  // and it then refuses to answer.
  @volatile private var defaultParallelism = 0

  override def onJobStart(e: SparkListenerJobStart): Unit = {
    try SparkContext.getActive.foreach(sc => defaultParallelism = sc.defaultParallelism)
    catch { case NonFatal(_) => }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case end: SparkListenerSQLExecutionEnd if end.qe != null =>
      // Capture must never fail or slow the application: anything unexpected is logged and dropped.
      try {
        ReplayCapture.events(end.executionId, end.qe, writerStage.get, defaultParallelism)
          .foreach(emit)
      } catch {
        case NonFatal(e) => logWarning(s"Replay capture skipped execution ${end.executionId}", e)
      }
    case _ =>
  }
}

/**
 * Writes the replay events of every SQL execution into the event log. Enable per application
 * with `spark.extraListeners=org.apache.spark.sql.replay.ReplayCaptureListener`.
 *
 * An event a listener posts back to the bus is dropped once the application is stopping, and a
 * batch job stops right after its last query. So on its first event this listener puts a
 * [[ReplayCapturer]] on the event log's own queue, which is drained at stop, and that writes
 * through the event logger on the logger's thread. Without event logging nothing is captured.
 */
class ReplayCaptureListener extends SparkFirehoseListener {
  private val installed = new AtomicBoolean

  override def onEvent(event: SparkListenerEvent): Unit = {
    // The first events can arrive while the context is still being constructed and not yet
    // active; keep looking until it is.
    if (!installed.get) {
      SparkContext.getActive.foreach { sc =>
        if (installed.compareAndSet(false, true)) {
          sc.eventLogger.foreach { logger =>
            sc.listenerBus.addToEventLogQueue(new ReplayCapturer(logger.onOtherEvent))
          }
        }
      }
    }
  }
}
