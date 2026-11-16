/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults

/**
 * The executor sizes, in cores, that executors of the default resource profile may take
 * (SPARK-61258). Heap, off-heap and PySpark memory per core are the same at every size.
 *
 * @param rungs sizes in cores, largest first
 * @param referenceCores `spark.executor.cores`; one requested executor is worth this many cores
 */
private[spark] case class ShapeLadder(rungs: Seq[Int], referenceCores: Int) {

  val smallest: Int = rungs.last

  /** The largest size below `cores`, or the smallest size if there is none. */
  def rungBelow(cores: Int): Int = rungs.find(_ < cores).getOrElse(smallest)

  /**
   * Pod sizes that cover `remainingCores`: each pod is the largest size that fits both
   * `ceilingCores` and what is still missing. A remainder below the smallest size is covered
   * by one pod of the smallest size.
   */
  def greedyFill(ceilingCores: Int, remainingCores: Int): Seq[Int] = {
    val sizes = new ArrayBuffer[Int]
    var left = remainingCores
    while (left > 0) {
      val size = rungs.find(r => r <= ceilingCores && r <= left).getOrElse(smallest)
      sizes += size
      left -= size
    }
    sizes.toSeq
  }
}

private[spark] object ShapeLadder {

  private val GANG_SCHEDULERS = Set("volcano", "yunikorn")

  /**
   * Parses and validates the shape ladder. Returns None when no ladder is configured.
   * Throws SparkException for a ladder that cannot be honored, so that a bad configuration
   * fails when the SparkContext is created and not when the first pod is requested.
   */
  def fromConf(conf: SparkConf): Option[ShapeLadder] = {
    conf.get(KUBERNETES_EXECUTOR_SHAPE_LADDER).map { configured =>
      val key = KUBERNETES_EXECUTOR_SHAPE_LADDER.key
      val rungs = configured.sorted(Ordering[Int].reverse)
      val referenceCores = conf.get(EXECUTOR_CORES)
      val taskCpus = conf.get(CPUS_PER_TASK)

      def reject(reason: String): Nothing =
        throw new SparkException(s"Invalid $key=${configured.mkString(",")}: $reason")

      if (rungs.distinct.size != rungs.size) {
        reject("sizes must be distinct.")
      }
      rungs.find(_ % referenceCores != 0).foreach { rung =>
        reject(s"$rung is not a multiple of ${EXECUTOR_CORES.key}=$referenceCores.")
      }
      if (rungs.last < taskCpus) {
        reject(s"the smallest size ${rungs.last} is below ${CPUS_PER_TASK.key}=$taskCpus.")
      }
      conf.get(KUBERNETES_EXECUTOR_SHAPE_INITIAL_CORES).filterNot(rungs.contains).foreach { c =>
        reject(s"${KUBERNETES_EXECUTOR_SHAPE_INITIAL_CORES.key}=$c is not one of the sizes.")
      }
      if (conf.get(KUBERNETES_ALLOCATION_PODS_ALLOCATOR) != "direct") {
        reject(s"only ${KUBERNETES_ALLOCATION_PODS_ALLOCATOR.key}=direct supports it.")
      }
      if (conf.contains(KUBERNETES_EXECUTOR_LIMIT_CORES)) {
        reject(s"${KUBERNETES_EXECUTOR_LIMIT_CORES.key} is a per-executor absolute and cannot " +
          "be combined with it.")
      }
      conf.getAllWithPrefix("spark.executor.resource.").collectFirst {
        case (k, _) if k.endsWith(".amount") => s"spark.executor.resource.$k"
      }.foreach { k =>
        reject(s"$k is a per-executor absolute and cannot be combined with it.")
      }
      val schedulerName =
        conf.get(KUBERNETES_EXECUTOR_SCHEDULER_NAME).orElse(conf.get(KUBERNETES_SCHEDULER_NAME))
      if (schedulerName.exists(GANG_SCHEDULERS.contains) ||
          conf.get(KUBERNETES_EXECUTOR_POD_FEATURE_STEPS)
            .exists(_.endsWith("VolcanoFeatureStep"))) {
        reject("gang scheduling (Volcano, YuniKorn) sizes its pod group for one executor size.")
      }
      ShapeLadder(rungs, referenceCores)
    }
  }

  def overheadFactor(conf: SparkConf): Double = {
    if (conf.contains(EXECUTOR_MEMORY_OVERHEAD_FACTOR)) {
      conf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR)
    } else {
      conf.get(MEMORY_OVERHEAD_FACTOR)
    }
  }

  /**
   * Resources of an executor of `cores` cores, given the resources of the reference shape.
   * Heap, off-heap and PySpark memory scale with the cores. An explicitly configured overhead
   * is the reference shape's overhead and scales too; otherwise it is recomputed from the
   * factor and the floor, as for an executor of that size without a ladder. A small size may
   * so carry a little more overhead per core than a large one.
   */
  def resourcesFor(
      reference: ExecutorResourcesOrDefaults,
      cores: Int,
      conf: SparkConf): ExecutorResourcesOrDefaults = {
    val referenceCores = reference.cores.get
    def scale(mib: Long): Long = mib * cores / referenceCores
    val heapMiB = scale(reference.executorMemoryMiB)
    val overheadMiB = if (conf.contains(EXECUTOR_MEMORY_OVERHEAD)) {
      scale(reference.memoryOverheadMiB)
    } else {
      math.max((overheadFactor(conf) * heapMiB).toLong, conf.get(EXECUTOR_MIN_MEMORY_OVERHEAD))
    }
    val offHeapMiB = scale(reference.memoryOffHeapMiB)
    val pysparkMiB = scale(reference.pysparkMemoryMiB)
    reference.copy(
      cores = Some(cores),
      executorMemoryMiB = heapMiB,
      memoryOffHeapMiB = offHeapMiB,
      pysparkMemoryMiB = pysparkMiB,
      memoryOverheadMiB = overheadMiB,
      totalMemMiB = heapMiB + overheadMiB + offHeapMiB + pysparkMiB)
  }
}
