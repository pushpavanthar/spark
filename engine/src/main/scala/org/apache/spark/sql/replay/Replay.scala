package org.apache.spark.sql.replay

import java.util.concurrent.CompletableFuture

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.{MapOutputStatistics, MapOutputTrackerMaster, SPARK_VERSION, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

case class Resolved(key: String, shuffleId: Int, numMappers: Int, bytes: Array[Long], dataSize: BigInt,
    rowCount: Option[BigInt])

/** The replay events of one SQL execution. */
case class QueryCapture(plan: SparkListenerSQLReplayPlan,
    shuffles: Seq[SparkListenerSQLShuffleStageStats])

/** State of the replay in progress. Not replaying means the rule is a no-op. */
object Ctx {
  @volatile var active = false
  val flags = mutable.LinkedHashSet[String]()
  private val resolved = mutable.Map[String, Resolved]()
  private var nextShuffleId = 1000000
  private var leafOf = Map[Long, Int]()
  private var shuf = Map[String, SparkListenerSQLShuffleStageStats]()
  private var bx = Map[String, Statistics]()
  private var rows = Map[String, Long]()

  private def tracker: MapOutputTrackerMaster =
    SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private def known(n: Long): Option[BigInt] = if (n < 0) None else Some(BigInt(n))

  def start(cap: QueryCapture, logical: LogicalPlan): Unit = synchronized {
    stop()
    leafOf = logical.collectLeaves().zipWithIndex
      .flatMap { case (l, i) => l.output.map(_.exprId.id -> i) }.toMap
    shuf = cap.shuffles.map(s => s.key -> s).toMap
    val p = cap.plan
    bx = p.broadcastKeys.indices.map { i =>
      p.broadcastKeys(i) -> Statistics(BigInt(p.broadcastDataSize(i)), known(p.broadcastRows(i)))
    }.toMap
    rows = p.rowKeys.zip(p.rowValues).toMap
    active = true
  }

  def stop(): Unit = synchronized {
    active = false
    resolved.values.foreach(r => tracker.unregisterShuffle(r.shuffleId))
    resolved.clear(); flags.clear()
  }

  /** Per-map sizes as [map][partition]: recorded for skew candidates, an even split elsewhere. */
  private def mapSizes(rec: SparkListenerSQLShuffleStageStats): Array[Array[Long]] = {
    val m = math.max(rec.numMappers, 1)
    val out = Array.tabulate(m, rec.bytesByPartitionId.length)((_, p) => rec.bytesByPartitionId(p) / m)
    rec.mapSizePartitions.indices.foreach { i =>
      val sizes = rec.mapSizes(i)
      sizes.indices.foreach(map => out(map)(rec.mapSizePartitions(i)) = sizes(map))
    }
    out
  }

  /** Recorded statistics for the shuffle this exchange would produce, at its partition count. */
  def shuffle(e: ShuffleExchangeExec): Resolved = synchronized {
    // ShuffleExchangeExec.numPartitions builds the shuffle dependency, which executes the child.
    val n = e.outputPartitioning.numPartitions
    val key = Keys.shufKey(e.child, e.outputPartitioning, leafOf)
    resolved.getOrElseUpdate(key + "#" + n, {
      nextShuffleId += 1
      val id = nextShuffleId
      shuf.get(key) match {
        case Some(rec) if rec.bytesByPartitionId.isEmpty =>
          Resolved(key, id, 0, null, BigInt(rec.dataSize), known(rec.rowCount))
        case Some(rec) if rec.numPartitions == n =>
          checkMapSizes(rec.bytesByPartitionId, rec.mapSizePartitions.toSet)
          register(id, mapSizes(rec))
          Resolved(key, id, rec.numMappers, rec.bytesByPartitionId, BigInt(rec.dataSize),
            known(rec.rowCount))
        case Some(rec) if rec.numPartitions % n == 0 &&
            e.outputPartitioning.isInstanceOf[HashPartitioning] =>
          // pmod(hash, n): partition p of n is the union of recorded partitions congruent to p.
          // Exact in rows; bytes measured up to 14% off a real run, hence the flag.
          def fold(a: Array[Long]): Array[Long] = {
            val out = new Array[Long](n); a.indices.foreach(i => out(i % n) += a(i)); out
          }
          flags += "FOLDED_PARTITIONS"
          checkMapSizes(fold(rec.bytesByPartitionId), rec.mapSizePartitions.map(_ % n).toSet)
          register(id, mapSizes(rec).map(fold))
          Resolved(key, id, rec.numMappers, fold(rec.bytesByPartitionId), BigInt(rec.dataSize),
            known(rec.rowCount))
        case Some(rec) =>
          flags += "EXTRAPOLATED_PARTITIONS"
          val bytes = Array.fill(n)(rec.bytesByPartitionId.sum / n)
          val mappers = math.max(rec.numMappers, 1)
          register(id, Array.fill(mappers)(bytes.map(_ / mappers)))
          Resolved(key, id, rec.numMappers, bytes, BigInt(rec.dataSize), known(rec.rowCount))
        case None =>
          // The run never shuffled this subplan (it broadcast it, say): no distribution exists.
          flags += "UNSEEN_SHUFFLE"
          val est = e.child.logicalLink.map(_.stats.sizeInBytes).getOrElse(BigInt(1))
          val perPartition = est / n
          val bytes = Array.fill(n)(
            if (perPartition.isValidLong) perPartition.toLong else Long.MaxValue)
          register(id, Array(bytes))
          Resolved(key, id, 1, bytes, est, None)
      }
    })
  }

  // Capture keeps per-map sizes for the largest skew candidates only. If the replayed skew
  // settings would call a partition skewed that has none, its split cannot be trusted.
  private def checkMapSizes(bytes: Array[Long], recorded: Set[Int]): Unit = {
    val conf = SQLConf.get
    val limit = math.max(conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD),
      (Utils.median(bytes, false) * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR)).toLong)
    if (bytes.indices.exists(i => bytes(i) > limit && !recorded(i))) flags += "MISSING_MAP_SIZES"
  }

  private def register(id: Int, mapSizes: Array[Array[Long]]): Unit = {
    val n = mapSizes.headOption.map(_.length).getOrElse(0)
    tracker.registerShuffle(id, mapSizes.length, n)
    mapSizes.zipWithIndex.foreach { case (sizes, m) =>
      tracker.registerMapOutput(id, m, MapStatus(BlockManagerId("replay", "localhost", 7337), sizes, m))
    }
  }

  def broadcast(e: BroadcastExchangeExec): Statistics = synchronized {
    val sub = Keys.subtree(e.child, leafOf)
    bx.get(sub)
      // The same data, recorded as a shuffle.
      .orElse(shuf.collectFirst { case (k, s) if k.startsWith(sub + "|") =>
        Statistics(BigInt(s.dataSize), known(s.rowCount)) })
      .orElse(rows.get(sub).map { n =>
        // Without a row count (no CBO) the size estimate says nothing per row; use the schema.
        val perRow = e.child.logicalLink.map { l =>
          l.stats.rowCount.map(r => l.stats.sizeInBytes / r.max(1))
            .getOrElse(EstimationUtils.getSizePerRow(l.output))
        }
        Statistics(perRow.getOrElse(BigInt(8)) * n + 1, Some(BigInt(n))) })
      .getOrElse {
        flags += "UNSEEN_BROADCAST"
        Statistics(e.child.logicalLink.map(_.stats.sizeInBytes).getOrElse(BigInt(1)), None)
      }
  }
}

case class FakeShuffleExchangeExec(delegate: ShuffleExchangeExec) extends ShuffleExchangeLike {
  @transient private lazy val rec = Ctx.shuffle(delegate)
  /** Key of the recorded shuffle this exchange stands for, see [[Keys.shufKey]]. */
  def recordedKey: String = rec.key
  /** Bytes per reduce partition, at this exchange's partition count; null without map output. */
  def bytesByPartitionId: Array[Long] = rec.bytes
  override def child: SparkPlan = delegate.child
  override def numMappers: Int = rec.numMappers
  // What the partitioner created, which a range partitioner may make smaller than requested.
  override def numPartitions: Int =
    if (rec.bytes != null) rec.bytes.length else delegate.outputPartitioning.numPartitions
  override def advisoryPartitionSize: Option[Long] = delegate.advisoryPartitionSize
  override def shuffleOrigin: ShuffleOrigin = delegate.shuffleOrigin
  override def outputPartitioning: Partitioning = delegate.outputPartitioning
  override protected def mapOutputStatisticsFuture: Future[MapOutputStatistics] =
    Future.successful(if (rec.bytes == null) null else new MapOutputStatistics(rec.shuffleId, rec.bytes))
  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] =
    throw new UnsupportedOperationException("replay never executes")
  override def runtimeStatistics: Statistics = Statistics(rec.dataSize, rec.rowCount)
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("replay never executes")
  override protected def doCanonicalize(): SparkPlan =
    FakeShuffleExchangeExec(delegate.canonicalized.asInstanceOf[ShuffleExchangeExec])
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(delegate = delegate.copy(child = newChild))
}

case class FakeBroadcastExchangeExec(delegate: BroadcastExchangeExec) extends BroadcastExchangeLike {
  @transient private lazy val stats = Ctx.broadcast(delegate)
  override def child: SparkPlan = delegate.child
  override def outputPartitioning: Partitioning = delegate.outputPartitioning
  override def relationFuture: java.util.concurrent.Future[Broadcast[Any]] =
    CompletableFuture.completedFuture[Broadcast[Any]](null)
  override protected def completionFuture: Future[Broadcast[Any]] = Future.successful(null)
  override def runtimeStatistics: Statistics = stats
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("replay never executes")
  override protected def doCanonicalize(): SparkPlan =
    FakeBroadcastExchangeExec(delegate.canonicalized.asInstanceOf[BroadcastExchangeExec])
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(delegate = delegate.copy(child = newChild))
}

case class ReplayExchangeRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = if (!Ctx.active) plan else plan.transformUp {
    case e: ShuffleExchangeExec => FakeShuffleExchangeExec(e)
    case e: BroadcastExchangeExec => FakeBroadcastExchangeExec(e)
  }
}

object Replay {
  /** Register on the engine's session: `SparkSession.builder().withExtensions(Replay.install)`. */
  def install(e: SparkSessionExtensions): Unit = e.injectQueryStagePrepRule(s => ReplayExchangeRule(s))

  /**
   * Predicts the final physical plan of the captured query under the session's current SQL conf.
   * Runs the real planner and the real AQE loop; exchanges are replaced by fakes that return
   * recorded statistics, so no job runs. Returns the plan and the flags raised.
   */
  def replay(spark: SparkSession, cap: QueryCapture): (SparkPlan, Seq[String]) = {
    if (cap.plan.sparkVersion != SPARK_VERSION) {
      throw new ReplayUnsupportedException(
        s"captured on Spark ${cap.plan.sparkVersion}, engine is $SPARK_VERSION")
    }
    val logical = SkeletonCodec.decode(cap.plan.root, spark)
    // A coalesce leaves at least the application's default parallelism, which on a cluster is
    // its executor cores. The engine's own is unrelated, so the recorded one is pinned.
    val conf = spark.sessionState.conf
    val pinParallelism = cap.plan.defaultParallelism > 0 &&
      conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST) &&
      conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM).isEmpty
    if (pinParallelism) {
      conf.setConfString(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key,
        cap.plan.defaultParallelism.toString)
    }
    Ctx.start(cap, logical)
    try {
      if (cap.plan.opaqueNodes > 0) Ctx.flags += "OPAQUE_NODE"
      // The plan was optimized by the run that recorded it; plan it as it is.
      val qe = new QueryExecution(spark, logical) {
        override lazy val optimizedPlan: LogicalPlan = logical
      }
      val plan = qe.executedPlan match {
        case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
        case p => p
      }
      (plan, Ctx.flags.toSeq)
    } finally {
      Ctx.stop()
      if (pinParallelism) conf.unsetConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM)
    }
  }
}
