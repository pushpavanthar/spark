#!/bin/sh
# run.sh <run-name> [key=value ...]: submits one run of the shuffle-heavy workload (broadcast
# joins off) and waits up to 8.5 minutes for it; prints its state and application id.
K="kubectl --context inmobi-jumpbox.teleport.sh-gke-cp-nonprod-eus-spark --request-timeout=60s -n spark-operator"
n=$1; shift
cd "$(dirname "$0")"
if ! $K get sparkapplication spark-replay-$n >/dev/null 2>&1; then
  REPLAY_SCALE=${REPLAY_SCALE:-50} python3 submit.py $n run spark.sql.autoBroadcastJoinThreshold=-1 "$@" \
    | $K apply -f - >/dev/null 2>&1
fi
i=0
while [ $i -lt 51 ]; do
  s=$($K get sparkapplication spark-replay-$n -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
  case "$s" in COMPLETED|FAILED|SUBMISSION_FAILED) break;; esac
  python3 -c "import time; time.sleep(10)"; i=$((i+1))
done
echo "$n $s $($K get sparkapplication spark-replay-$n -o jsonpath='{.status.sparkApplicationId}' 2>/dev/null)"
