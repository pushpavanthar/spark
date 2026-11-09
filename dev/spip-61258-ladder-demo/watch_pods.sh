#!/usr/bin/env bash
# Samples executor pods every 10s for the given number of seconds (default 150).
DUR=${1:-150}; END=$((SECONDS+DUR))
while [ $SECONDS -lt $END ]; do
  echo "=== t+${SECONDS}s"
  kubectl --context kwok-spip get pods -l spark-role=executor \
    -o custom-columns='POD:.metadata.name,EXEC:.metadata.labels.spark-exec-id,CORES:.metadata.labels.spark-exec-shape-cores,CPU_REQ:.spec.containers[0].resources.requests.cpu,MEM_REQ:.spec.containers[0].resources.requests.memory,PHASE:.status.phase,NODE:.spec.nodeName' 2>&1
  sleep 10
done
