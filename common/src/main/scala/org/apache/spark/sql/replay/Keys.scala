package org.apache.spark.sql.replay

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.util.Utils

/**
 * Keys that identify a subplan in the statistics recorded by a run. The same code computes them
 * on the real physical plan at capture time and on the re-planned one at replay time, so they
 * must only use what survives the round trip: leaf positions, expression ids and names.
 */
object Keys {
  def strip(p: SparkPlan): SparkPlan = p match {
    case w: WholeStageCodegenExec => strip(w.child)
    case i: InputAdapter => strip(i.child)
    case c: ColumnarToRowExec => strip(c.child)
    case o => o
  }
  def leaves(p: SparkPlan): Seq[SparkPlan] = p match {
    case s: QueryStageExec => leaves(s.plan)
    case r: ReusedExchangeExec => leaves(r.child)
    case a: AdaptiveSparkPlanExec => leaves(a.executedPlan)
    case l if l.children.isEmpty => Seq(l)
    case o => o.children.flatMap(leaves)
  }
  def leafSet(p: SparkPlan, m: Map[Long, Int]): String =
    leaves(p).map(l => l.output.headOption.flatMap(a => m.get(a.exprId.id)).getOrElse(-1))
      .sorted.mkString(",")
  // Literal values are masked in the skeleton, so a key must read the same with any value.
  private def show(e: Expression): String =
    e.transform { case l: Literal => Literal(null, l.dataType) }.toString
  def partSig(p: Partitioning): String = p match {
    case h: HashPartitioning => "hash:" + h.expressions.map(show).mkString(",")
    case SinglePartition => "single"
    case r: RangePartitioning => "range:" + r.ordering.map(show).mkString(",")
    case o => o.getClass.getSimpleName
  }
  // A scan that prunes columns has no Project above it, a replayed RDD scan does, so the node
  // name differs between a real run and a replay. Partial and final aggregates share a logical
  // link and a node name; only the final one requires a distribution of its child.
  def name(p: SparkPlan): String = strip(p) match {
    case a: BaseAggregateExec =>
      a.nodeName + (if (a.requiredChildDistributionExpressions.isDefined) ":final" else ":partial")
    case _ => "*"
  }
  def link(p: SparkPlan): String = strip(p).logicalLink
    .map(l => l.nodeName + ":" + l.output.map(_.exprId.id).mkString(",")).getOrElse("-")
  def subtree(p: SparkPlan, m: Map[Long, Int]): String = leafSet(p, m) + "|" + name(p) + "|" + link(p)
  def shufKey(child: SparkPlan, part: Partitioning, m: Map[Long, Int]): String =
    subtree(child, m) + "|" + partSig(part)

  /** Capture records per-map sizes only for partitions this many times the median or larger. */
  val MapSizeMinRatio = 2.0
  val MapSizeMaxPartitions = 20

  /** Skew candidates among reduce partitions, largest first, and whether the list was cut. */
  def mapSizePartitions(bytes: Array[Long]): (Array[Int], Boolean) = {
    if (bytes.isEmpty) return (Array.empty, false)
    val threshold = Utils.median(bytes, false) * MapSizeMinRatio
    val candidates = bytes.indices.filter(i => bytes(i) > threshold)
      .sortBy(i => -bytes(i))
    (candidates.take(MapSizeMaxPartitions).toArray, candidates.size > MapSizeMaxPartitions)
  }
}
