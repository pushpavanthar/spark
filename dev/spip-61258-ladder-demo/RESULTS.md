# SPARK-61258 prototype: shape ladder on a KWOK cluster

Run on 2026-11-10 against a `kwokctl` fake cluster (no real nodes; pods are scheduled by a
real kube-scheduler and marked Running by KWOK; executors never connect to the driver).

## Setup

- Nodes: `big-16cpu` (16 cpu, 64Gi), `small-8cpu-a` and `small-8cpu-b` (8 cpu, 32Gi each). See `nodes.yaml`.
- Driver: `spark.executor.cores=4`, `spark.executor.memory=4g`, `spark.executor.instances=8`
  (32 cores wanted), `spark.kubernetes.executor.shape.ladder=16,8,4`,
  `spark.kubernetes.executor.shape.downshiftTimeout=30s`. See `run_driver.sh`.

## What happened

```
=== t+0s
POD                                EXEC   CORES   CPU_REQ   MEM_REQ   PHASE     NODE
spark-pi-7b0e1ca1c194d9d4-exec-1   1      16      16        18022Mi   Running   big-16cpu
spark-pi-7b0e1ca1c194d9d4-exec-2   2      16      16        18022Mi   Pending   <none>
=== t+51s
POD                                EXEC   CORES   CPU_REQ   MEM_REQ   PHASE     NODE
spark-pi-7b0e1ca1c194d9d4-exec-1   1      16      16        18022Mi   Running   big-16cpu
spark-pi-7b0e1ca1c194d9d4-exec-3   3      8       8         9011Mi    Running   small-8cpu-a
spark-pi-7b0e1ca1c194d9d4-exec-4   4      8       8         9011Mi    Running   small-8cpu-b
=== t+152s
POD                                EXEC   CORES   CPU_REQ   MEM_REQ   PHASE     NODE
spark-pi-7b0e1ca1c194d9d4-exec-1   1      16      16        18022Mi   Running   big-16cpu
spark-pi-7b0e1ca1c194d9d4-exec-3   3      8       8         9011Mi    Running   small-8cpu-a
spark-pi-7b0e1ca1c194d9d4-exec-4   4      8       8         9011Mi    Running   small-8cpu-b
done
```

Driver log (ExecutorPodsAllocator):

```
Going to request 2 executors from Kubernetes for ResourceProfile Id: 0, target: 32, known: 0, sharedSlotFromPendingPods: 20.
Executor 2 pod spark-pi-7b0e1ca1c194d9d4-exec-2 cannot be scheduled: 0/3 nodes are available: 3 Insufficient cpu. no new claims to deallocate, preemption: 0/3 nodes are available: 1 No preemption vict
Executor pods 2 could not be scheduled for lack of capacity; moving the shape ladder down to 8 cores per executor.
Going to request 2 executors from Kubernetes for ResourceProfile Id: 0, target: 32, known: 16, sharedSlotFromPendingPods: 20.
```

## Reading

- Rung 1 (16 cores): two pods requested for 32 cores. One lands on the 16-cpu node; the other is
  Unschedulable with `3 Insufficient cpu`.
- After the 30 s timeout the allocator deletes the stuck pod, moves to rung 2 (8 cores) and
  re-requests the missing 16 cores as two 8-core pods, which land on the two 8-cpu nodes.
- Final fill: 16 + 8 + 8 = 32 cores, exactly the target. Heap per core is constant: the 16-core
  pod requests 18022Mi (16g heap + 10% overhead) and the 8-core pods 9011Mi.
- For comparison, a homogeneous 16-core setting strands both 8-cpu nodes and never reaches the
  target; a homogeneous 4-core setting reaches it with 8 JVMs instead of 3.

## Reproduce

```
brew install kwok
kwokctl create cluster --name spip --wait 180s
kubectl --context kwok-spip apply -f dev/spip-61258-ladder-demo/nodes.yaml
build/sbt -Pkubernetes package examples/package      # JDK 21
dev/spip-61258-ladder-demo/run_driver.sh &            # leave it running
dev/spip-61258-ladder-demo/watch_pods.sh 160
kwokctl delete cluster --name spip
```
