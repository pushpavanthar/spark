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
package org.apache.spark.scheduler.cluster.k8s

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_SHAPE_LADDER
import org.apache.spark.internal.config._

class KubernetesExecutorBackendSuite extends SparkFunSuite {

  test("SPARK-61258: the executor conf describes the pod under a shape ladder") {
    def driverConf: SparkConf = new SparkConf(false)
      .set(EXECUTOR_CORES, 4)
      .set(EXECUTOR_MEMORY.key, "8g")
      .set(MEMORY_OFFHEAP_SIZE.key, "1g")

    val ladder = driverConf.set(KUBERNETES_EXECUTOR_SHAPE_LADDER.key, "16,8,4")
    KubernetesExecutorBackend.setShapeConfs(ladder, 16, Some("32768m"))
    assert(ladder.get(EXECUTOR_CORES) === 16)
    assert(ladder.get(EXECUTOR_MEMORY) === 32768)
    assert(ladder.get(MEMORY_OFFHEAP_SIZE) === 4L * 1024 * 1024 * 1024)

    val noLadder = driverConf
    KubernetesExecutorBackend.setShapeConfs(noLadder, 16, Some("32768m"))
    assert(noLadder.get(EXECUTOR_CORES) === 4)
    assert(noLadder.get(EXECUTOR_MEMORY) === 8192)
  }
}
