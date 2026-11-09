#!/usr/bin/env bash
# Runs a Spark driver locally against the KWOK fake cluster. Executors never start (fake
# nodes), so the driver keeps its pods pending/running forever; we observe the allocator.
set -euo pipefail
export JAVA_HOME=${JAVA_HOME:?set JAVA_HOME to a JDK 21}
export PATH=$JAVA_HOME/bin:$PATH
export KUBECONFIG=$HOME/.kube/config
kubectl config use-context kwok-spip >/dev/null
API=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
WT=$(cd "$(dirname "$0")/../.." && pwd)
EX=$(ls $WT/examples/target/scala-2.13/jars/spark-examples_2.13-*.jar | head -1)
cd $WT
exec bin/spark-submit \
  --master "k8s://$API" --deploy-mode client \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.app.name=spip-ladder-demo \
  --conf spark.kubernetes.container.image=fake/spark:prototype \
  --conf spark.kubernetes.namespace=default \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.instances=8 \
  --conf spark.kubernetes.executor.shape.ladder=16,8,4 \
  --conf spark.kubernetes.executor.shape.downshiftTimeout=30s \
  --conf spark.kubernetes.allocation.batch.size=10 \
  --conf spark.kubernetes.allocation.batch.delay=2s \
  --conf spark.kubernetes.allocation.maxPendingPods=20 \
  "$EX" 100000
