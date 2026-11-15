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

Only the recorded run is exported. A trace rewritten for a config what-if (new partition counts
from the plan replay) is not built.

## Flags

A prediction carries a flag whenever it used something the run did not record.

| Flag | Meaning |
|---|---|
| `FOLDED_PARTITIONS` | Partition count is a divisor of the recorded one; byte sizes are approximate (up to ~14% off) |
| `EXTRAPOLATED_PARTITIONS` | Partition count does not divide the recorded one; sizes are a uniform guess |
| `UNSEEN_SHUFFLE` / `UNSEEN_BROADCAST` | The run never materialized this subplan that way; statistics are estimated |
| `MISSING_MAP_SIZES` | The replayed skew settings call a partition skewed for which no per-map sizes were recorded |
| `OPAQUE_NODE` | The skeleton holds an expression the codec could not represent (a UDF, a set of values) |

## Not built yet

Distiller, cost model, scheduler simulation, pack store, service API, LLM loop. See the plan's
milestones M3 to M7. `docs/prototype/results.tsv` is the experiment log of the prototype.
