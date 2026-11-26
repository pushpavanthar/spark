# spark-replay

Replays a finished Spark SQL run under changed configs without data and without executing
anything, so config and resource candidates can be tried in milliseconds.

Design: "Spark Replay Archive High Level Design" (Confluence page 122114801818).
Built for Spark 3.5.8 / Scala 2.12. One build per Spark version: the code uses `private[spark]` classes.

## Modules

| Module | What | Ships where |
|---|---|---|
| `common` | The two event classes, subplan keys, skeleton codec (plan <-> JSON) | application image and engine |
| `capture` | `ReplayCaptureListener`: writes the events into the event log at query end | application image |
| `engine` | Rebuilds the plan and drives Spark's planner and AQE loop over recorded statistics | replay service |
| `fidelity` | Tests only: real runs as ground truth, the gate, capture tests | nowhere |

## Enabling capture

    --jars common.jar,capture.jar
    --conf spark.extraListeners=org.apache.spark.sql.replay.ReplayCaptureListener

Capture never fails a query: anything unexpected is logged and that execution is skipped.
It writes into the event log, so `spark.eventLog.enabled` must be on; without it nothing is captured.

## Tests

    sbt test                                  # everything; first run generates data and ground truth (~3 min)
    sbt "fidelity/testOnly *FidelityGateSuite"
    sbt "fidelity/testOnly *CaptureSuite"

`FidelityGateSuite` is the gate for every change. For each of 9 queries it captures one real
baseline run, then for each of 21 config variants compares the replayed final plan with the plan a
real run under that variant ended with (189 pairs). It requires that no prediction without a flag
is wrong, and an exact-match rate of at least 0.90 (measured: 0.93). Details of the last run:
`fidelity/target/fidelity/<version>/last_eval.txt`.

Changing `Harness.queries`, `variants` or `base` changes the harness version and regenerates the
ground truth. Bump `Harness.dataVersion` when `gen` changes.

## Synthetic jobs

    sbt "fidelity/testOnly *SyntheticJobSuite"

Three whole jobs written and fed by the suite itself, with no external code, data or cluster: a
nested-event explode and aggregate into an hourly-partitioned Iceberg table, a wide rollup from
Iceberg into Iceberg at 12000 shuffle partitions, and a union of three inputs joined to dimensions
and a skewed fact. Each checks that a baseline replay gives the recorded plan with no flag and no
opaque node, and that five config what-ifs give the plan of a real rerun or carry a flag.

The engine needs the Iceberg runtime jar on its classpath to replay plans that write Iceberg
tables. Not covered: Comet.

## Simulator trace export

`AroTrace.export(eventLog, outDir)` writes `outDir/<app-id>/trace.json.gz` in the trace format of the
adaptive-resource-simulator (`aro_sim`, schema 1), so its `simulate` and `check` commands can replay a
run straight from an event log. Stage parents are the event log's exact ones, written under the
simulator's field name `parents_inferred`. All Spark properties are exported, not only the ones the
simulator reads. In local mode the driver is exported as the only executor.

```
spark-submit --class org.apache.spark.sql.replay.AroTrace distiller.jar <event-log> <out-dir>
```

`WhatIfTrace.export(spark, eventLog, conf, outDir, name)` writes the same trace rewritten for a
candidate SQL configuration. Every captured query is replayed under its recorded and under the
candidate configuration; each stage whose shuffle read is cut differently gets the candidate's task
count, and its recorded work is shared out by the bytes of each new partition (fetch wait grows with
the task count as in the simulator's own cost model). The trace gains a `what_if` section with the
flags raised and the stages re-cut.

- Only repartitioning is exported. A candidate that changes a plan's shape (a join strategy, an
  exchange) is refused.
- The log must come from a run with capture enabled.
- No per-task fixed cost is modelled: a candidate with many more, smaller tasks is predicted too fast.

## Flags

A prediction carries a flag whenever it used something the run did not record.

| Flag | Meaning |
|---|---|
| `FOLDED_PARTITIONS` | Partition count is a divisor of the recorded one; byte sizes are approximate (up to ~14% off) |
| `EXTRAPOLATED_PARTITIONS` | Partition count does not divide the recorded one; sizes are a uniform guess |
| `UNSEEN_SHUFFLE` / `UNSEEN_BROADCAST` | The run never materialized this subplan that way; statistics are estimated |
| `MISSING_MAP_SIZES` | The replayed skew settings call a partition skewed for which no per-map sizes were recorded |
| `BASELINE_MISMATCH` | What-if trace only: replayed under its recorded configuration, a query does not give the plan it really ended with |
| `UNCAPTURED_EXECUTION` | What-if trace only: a query has no captured plan, so its stages stay as recorded |
| `OPAQUE_NODE` | The skeleton holds an expression the codec could not represent (a UDF, a set of values) |

## Cluster validation

`cluster/submit.py` prints a SparkApplication that runs `cluster/replay_validation.py` (the
harness tables and queries, scaled) with capture enabled. `ClusterValidationSuite` takes the event
logs of a baseline and of candidate runs (`REPLAY_CLUSTER_LOGS`), rewrites the baseline for each
candidate and compares it with the real rerun.

Measured on 3 executors of 5 cores, 200M rows, broadcast joins off (baselines of 6.5 and 8
minutes, repeating within 1%). Change in wall-clock against the baseline, real and predicted:

| Executor memory | Candidate | Real | Predicted | Used to shape the model |
|---|---|---|---|---|
| 24g | coalescing off | -1% | +2% | yes |
| 24g | shuffle partitions 400 to 50 | +3% | +4% | yes |
| 24g | skew join off | +32% | +42% | yes |
| 24g | advisory size 512m | +40% | +55% | yes |
| 24g | shuffle partitions 400 to 100 | +6% | +2% | no |
| 24g | skew threshold 1g | -2% | 0% | no |
| 24g | advisory size 256m | +3% | +12% | no |
| 24g | advisory size 512m and skew join off | +69% | +65% | no |
| 12g | advisory size 256m | +25% | +29% | no |
| 12g | skew join off | +33% | +51% | no |

How a re-cut task is priced:
- it needs the stage's working memory per byte (what the recorded tasks held plus what they
  spilled) times what it reads, and spills what exceeds its grant;
- tasks of an executor share its memory pool, but a task far larger than the rest outlives them
  and has the pool to itself;
- a spilled byte costs CPU, and waiting when tasks spilling together share a disk. Both rates
  are measured on the recorded run's own spilled tasks; without any, the CPU rate is the
  simulator's constant and the disk limit is unknown.

Known bias: just above the grant the rule predicts spill that does not happen (advisory 256m at
24g), and a lone giant task is priced too slow (skew join off).

## Not built yet

Pack store, service API, LLM loop. Time and resources are predicted by the simulator, not here. See the plan's
milestones M3 to M7. `docs/prototype/results.tsv` is the experiment log of the prototype.
