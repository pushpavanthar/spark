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

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults

class ShapeLadderSuite extends SparkFunSuite {

  // Reference shape: 4 cores, 8g. 2048 MiB of heap per core.
  private def baseConf: SparkConf = new SparkConf(false)
    .set(EXECUTOR_CORES, 4)
    .set(EXECUTOR_MEMORY.key, "8g")
    .set(KUBERNETES_EXECUTOR_SHAPE_LADDER.key, "8,16,4")

  private def assertRejected(conf: SparkConf, fragment: String): Unit = {
    val e = intercept[SparkException](ShapeLadder.fromConf(conf))
    assert(e.getMessage.contains(KUBERNETES_EXECUTOR_SHAPE_LADDER.key))
    assert(e.getMessage.contains(fragment), e.getMessage)
  }

  test("no ladder configured") {
    assert(ShapeLadder.fromConf(new SparkConf(false)).isEmpty)
  }

  test("sizes are ordered largest first") {
    val ladder = ShapeLadder.fromConf(baseConf).get
    assert(ladder === ShapeLadder(Seq(16, 8, 4), 4))
    assert(ladder.smallest === 4)
    assert(ladder.rungBelow(16) === 8)
    assert(ladder.rungBelow(8) === 4)
    assert(ladder.rungBelow(4) === 4)
  }

  test("greedy fill") {
    val ladder = ShapeLadder(Seq(16, 8, 4), 4)
    assert(ladder.greedyFill(16, 32) === Seq(16, 16))
    assert(ladder.greedyFill(16, 28) === Seq(16, 8, 4))
    // The ceiling is a ceiling: one reference executor is one reference-size pod.
    assert(ladder.greedyFill(16, 4) === Seq(4))
    assert(ladder.greedyFill(8, 32) === Seq(8, 8, 8, 8))
    assert(ladder.greedyFill(16, 0) === Nil)
    // A remainder below the smallest size is covered by one pod of the smallest size.
    assert(ShapeLadder(Seq(16, 8), 4).greedyFill(16, 12) === Seq(8, 8))
  }

  test("sizes must be distinct multiples of the reference cores") {
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_SHAPE_LADDER.key, "16,8,8"), "distinct")
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_SHAPE_LADDER.key, "16,6"),
      "6 is not a multiple of spark.executor.cores=4")
  }

  test("the smallest size must hold one task") {
    assertRejected(baseConf.set(CPUS_PER_TASK, BigDecimal(8)), "below spark.task.cpus=8")
  }

  test("a size must be a multiple of the task cpus unless wasted slots are allowed") {
    val conf = baseConf.set(CPUS_PER_TASK, BigDecimal(3))
    assertRejected(conf, "is not a multiple of spark.task.cpus=3")
    assert(ShapeLadder.fromConf(
      conf.set(KUBERNETES_EXECUTOR_SHAPE_ALLOW_WASTED_SLOTS, true)).nonEmpty)
    assert(ShapeLadder.fromConf(baseConf.set(CPUS_PER_TASK, BigDecimal(0.5))).nonEmpty)
  }

  test("timeouts must be ordered") {
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_SHAPE_DOWNSHIFT_TIMEOUT.key, "10min"),
      KUBERNETES_ALLOCATION_EXECUTOR_TIMEOUT.key)
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_SHAPE_UPSHIFT_INTERVAL.key, "2h"),
      KUBERNETES_EXECUTOR_SHAPE_UPSHIFT_MAX_INTERVAL.key)
  }

  test("ladder navigation") {
    val ladder = ShapeLadder(Seq(16, 8, 4), 4)
    assert(ladder.rungAbove(4) === Some(8))
    assert(ladder.rungAbove(8) === Some(16))
    assert(ladder.rungAbove(16) === None)
  }

  test("initialCores must be one of the sizes") {
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_SHAPE_INITIAL_CORES, 12), "initialCores=12")
    assert(ShapeLadder.fromConf(baseConf.set(KUBERNETES_EXECUTOR_SHAPE_INITIAL_CORES, 8)).nonEmpty)
  }

  test("only the direct pods allocator supports a ladder") {
    Seq("statefulset", "deployment").foreach { allocator =>
      assertRejected(baseConf.set(KUBERNETES_ALLOCATION_PODS_ALLOCATOR, allocator), "direct")
    }
  }

  test("per-executor absolutes cannot be combined with a ladder") {
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_LIMIT_CORES, "4"),
      KUBERNETES_EXECUTOR_LIMIT_CORES.key)
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_REQUEST_CORES, "4"),
      KUBERNETES_EXECUTOR_REQUEST_CORES.key)
    assertRejected(baseConf.set("spark.executor.resource.gpu.amount", "1"),
      "spark.executor.resource.gpu.amount")
  }

  test("gang scheduling cannot be combined with a ladder") {
    assertRejected(baseConf.set(KUBERNETES_SCHEDULER_NAME, "volcano"), "gang scheduling")
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_SCHEDULER_NAME, "yunikorn"),
      "gang scheduling")
    assertRejected(baseConf.set(KUBERNETES_EXECUTOR_POD_FEATURE_STEPS.key,
      "org.apache.spark.deploy.k8s.features.VolcanoFeatureStep"), "gang scheduling")
  }

  test("a size that would need the overhead floor is rejected") {
    // 1g per core: 0.1 x heap reaches the 384m floor at 4 cores.
    val conf = baseConf.set(EXECUTOR_MEMORY.key, "4g")
      .set(KUBERNETES_EXECUTOR_SHAPE_LADDER.key, "8,4")
    assert(ShapeLadder.fromConf(conf).nonEmpty)
    val tooSmall = conf.set(EXECUTOR_CORES, 2).set(EXECUTOR_MEMORY.key, "2g")
      .set(KUBERNETES_EXECUTOR_SHAPE_LADDER.key, "8,4,2")
    assertRejected(tooSmall, "The smallest size this ratio allows is 4 cores")
    // An explicit overhead scales with the cores and has no floor to respect.
    assert(ShapeLadder.fromConf(tooSmall.set(EXECUTOR_MEMORY_OVERHEAD.key, "300m")).nonEmpty)
  }

  test("resources scale with the cores") {
    val reference = ExecutorResourcesOrDefaults(Some(4), 8192, 1024, 512, 819, 10547, Map.empty)
    val conf = baseConf
    val r = ShapeLadder.resourcesFor(reference, 16, conf)
    assert(r.cores === Some(16))
    assert(r.executorMemoryMiB === 32768)
    assert(r.memoryOffHeapMiB === 4096)
    assert(r.pysparkMemoryMiB === 2048)
    assert(r.memoryOverheadMiB === 3276)
    assert(r.totalMemMiB === 32768 + 4096 + 2048 + 3276)
    assert(ShapeLadder.resourcesFor(reference, 4, conf) === reference)

    val explicit = conf.set(EXECUTOR_MEMORY_OVERHEAD.key, "819m")
    assert(ShapeLadder.resourcesFor(reference, 8, explicit).memoryOverheadMiB === 1638)
  }
}
