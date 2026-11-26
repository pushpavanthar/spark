package org.apache.spark.sql.replay

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._

object Sig {
  /** Shape of a final plan: join strategies, exchanges, AQE reads, aggregates. No ids, no sizes. */
  def sig(p: SparkPlan): String = p match {
    case a: AdaptiveSparkPlanExec => sig(a.executedPlan)
    case s: QueryStageExec => sig(s.plan)
    case r: ReusedExchangeExec => "reused(" + sig(r.child) + ")"
    case j: SortMergeJoinExec =>
      s"SMJ${if (j.isSkewJoin) "*" else ""}(${sig(j.left)},${sig(j.right)})"
    case j: BroadcastHashJoinExec => s"BHJ[${j.buildSide}](${sig(j.left)},${sig(j.right)})"
    case j: ShuffledHashJoinExec =>
      s"SHJ${if (j.isSkewJoin) "*" else ""}[${j.buildSide}](${sig(j.left)},${sig(j.right)})"
    case r: AQEShuffleReadExec =>
      s"read[n=${r.partitionSpecs.length},local=${r.isLocalRead},skew=${r.hasSkewedPartition}]" +
        s"(${sig(r.child)})"
    case e: ShuffleExchangeLike => s"shuf[${e.numPartitions}](${sig(e.child)})"
    case e: BroadcastExchangeLike => s"bx(${sig(e.child)})"
    case a: BaseAggregateExec => s"agg(${sig(a.child)})"
    case s: SortExec => s"sort(${sig(s.child)})"
    case _: LocalTableScanExec => "local"
    case l if l.children.isEmpty => "scan"
    case o => o.children.map(sig).mkString(",")
  }
  /** Join strategies only. */
  def joins(s: String): String = "(SMJ\\*?|BHJ\\[\\w+\\]|SHJ\\*?\\[\\w+\\])".r.findAllIn(s).mkString(" ")
}
