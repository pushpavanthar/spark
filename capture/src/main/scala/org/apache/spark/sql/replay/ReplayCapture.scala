package org.apache.spark.sql.replay

import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, SPARK_VERSION, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
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

  def events(executionId: Long, qe: QueryExecution): Seq[SparkListenerEvent] = {
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
        val mapSizes = s.mapStats.flatMap(m => tracker.shuffleStatuses.get(m.shuffleId)).map { st =>
          st.withMapStatuses(ms => parts.map(p => ms.map(_.getSizeForBlock(p))))
        }
        SparkListenerSQLShuffleStageStats(executionId,
          Keys.shufKey(e.child, e.outputPartitioning, leafOf), e.numMappers,
          e.outputPartitioning.numPartitions,
          bytes, stats.rowCount.map(clamp).getOrElse(-1L), clamp(stats.sizeInBytes),
          if (mapSizes.isDefined) parts else Array.empty, mapSizes.getOrElse(Array.empty),
          truncated)
    }.groupBy(_.key).values.map(_.head).toSeq

    val broadcasts = nodes.collect {
      case b: BroadcastQueryStageExec
          if b.isMaterialized && b.plan.isInstanceOf[BroadcastExchangeLike] =>
        val stats = b.getRuntimeStatistics
        Keys.subtree(b.broadcast.child, leafOf) ->
          ((clamp(stats.sizeInBytes), stats.rowCount.map(clamp).getOrElse(-1L)))
    }.toMap.toSeq
    val rows = nodes.flatMap { n =>
      n.metrics.get("numOutputRows").map(v => Keys.subtree(n, leafOf) -> v.value)
    }.toMap.toSeq

    SparkListenerSQLReplayPlan(executionId, SPARK_VERSION, root, encoder.opaqueNodes,
      rows.map(_._1).toArray, rows.map(_._2).toArray,
      broadcasts.map(_._1).toArray, broadcasts.map(_._2._1).toArray,
      broadcasts.map(_._2._2).toArray) +: shuffles
  }
}

/**
 * Writes the replay events for every SQL execution into the event log. Enable per application
 * with `spark.extraListeners=org.apache.spark.sql.replay.ReplayCaptureListener`.
 */
class ReplayCaptureListener extends SparkListener with Logging {
  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case end: SparkListenerSQLExecutionEnd if end.qe != null =>
      // Capture must never fail or slow the application: anything unexpected is logged and dropped.
      try {
        val bus = end.qe.sparkSession.sparkContext.listenerBus
        ReplayCapture.events(end.executionId, end.qe).foreach(bus.post)
      } catch {
        case NonFatal(e) => logWarning(s"Replay capture skipped execution ${end.executionId}", e)
      }
    case _ =>
  }
}
