package org.apache.spark.sql.replay

import com.fasterxml.jackson.databind.JsonNode

import org.apache.spark.scheduler.SparkListenerEvent

/**
 * The optimized logical plan of one SQL execution, reduced to what planning depends on, plus the
 * row counts and broadcast statistics observed while it ran.
 *
 * Primitive collections are arrays, not Seq/Option/Map: jackson-module-scala erases their element
 * type and hands back Integer where Long is declared.
 *
 * @param root skeleton tree, see [[SkeletonCodec]]
 * @param opaqueNodes operators and expressions the codec could not represent
 * @param rowKeys / rowValues observed output rows per subplan key
 * @param broadcastKeys / broadcastDataSize / broadcastRows runtime statistics of each materialized
 *        broadcast; rows is -1 when unknown
 * @param defaultParallelism the application's default parallelism when the query ended, which is
 *        the least number of partitions a coalesce leaves; 0 when not recorded
 * @param finalPlan shape of the plan the run ended with, see [[Sig.sig]]; empty when not
 *        recorded. A replay under the recorded configuration that gives another shape cannot
 *        reproduce this run.
 * @param opKeys / opNodes / opMetrics / opValues every SQL metric of every operator that ran:
 *        subplan key, node name, metric name, value summed over tasks (times in the metric's
 *        own unit). What an operator cost, which task metrics do not tell.
 */
case class SparkListenerSQLReplayPlan(
    executionId: Long,
    sparkVersion: String,
    root: JsonNode,
    opaqueNodes: Int,
    rowKeys: Array[String],
    rowValues: Array[Long],
    broadcastKeys: Array[String],
    broadcastDataSize: Array[Long],
    broadcastRows: Array[Long],
    defaultParallelism: Int,
    finalPlan: String,
    opKeys: Array[String],
    opNodes: Array[String],
    opMetrics: Array[String],
    opValues: Array[Long]) extends SparkListenerEvent

/**
 * What AQE saw when one shuffle stage finished.
 *
 * @param key subplan key of the shuffle's input plus its partitioning, see [[Keys.shufKey]]
 * @param numPartitions the partition count the plan asked for. A range partitioner may create
 *        fewer; `bytesByPartitionId.length` is what it created
 * @param bytesByPartitionId empty when the shuffle had no map output
 * @param rowCount -1 when unknown
 * @param mapSizePartitions / mapSizes for each listed reduce partition, the bytes written by every
 *        map task. Only skew candidates are listed, see [[Keys.mapSizePartitions]]
 * @param mapSizesTruncated true when more partitions qualified than were recorded
 * @param writerStageId the scheduler stage that wrote the shuffle, which ties the plan to the
 *        stages and tasks of the event log; -1 when unknown
 */
case class SparkListenerSQLShuffleStageStats(
    executionId: Long,
    key: String,
    numMappers: Int,
    numPartitions: Int,
    bytesByPartitionId: Array[Long],
    rowCount: Long,
    dataSize: Long,
    mapSizePartitions: Array[Int],
    mapSizes: Array[Array[Long]],
    mapSizesTruncated: Boolean,
    writerStageId: Int) extends SparkListenerEvent
