"""Prints the SparkApplication for one run of replay_validation.py on gke-cp-nonprod-eus-spark.

  [REPLAY_SCALE=n] submit.py <run-name> <gen|run> [key=value ...]
                                     extra Spark conf for a candidate; scale 1 = 4M sales rows

The image is the public Apache Spark 3.5.8 one (the cluster cannot pull the production
spark-base), so the GCS connector is fetched by an init container. The job and the capture jars
come from the ConfigMap spark-replay-app.
"""
import json
import os
import sys

BUCKET = "gs://gcs-cp-nonprod-eus-spark-bucket/spark-replay"
IMAGE = "apache/spark:3.5.8-scala2.12-java17-python3-ubuntu"
CONNECTOR = ("https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/"
             "hadoop3-2.2.21/gcs-connector-hadoop3-2.2.21-shaded.jar")
# 10 gives one-minute runs, too short to tell candidates apart; 50 gives runs of minutes.
SCALE = os.environ.get("REPLAY_SCALE", "10")
DATA = BUCKET + "/data-" + SCALE

name, mode = sys.argv[1], sys.argv[2]
extra = dict(a.split("=", 1) for a in sys.argv[3:])

fetch = [{
    "name": "fetch-gcs-connector", "image": IMAGE, "command": ["python3", "-c"],
    "args": ["import urllib.request as u; u.urlretrieve('%s', '/opt/extra/gcs-connector.jar')"
             % CONNECTOR],
    "volumeMounts": [{"name": "extra", "mountPath": "/opt/extra"}],
}]


def pod(pool, **more):
    tolerations = [{"key": "service", "operator": "Equal", "value": pool, "effect": "NoSchedule"},
                   {"key": "cloud.google.com/compute-class", "operator": "Exists",
                    "effect": "NoSchedule"}]
    return dict(more, serviceAccount="spark-admin-service-account", initContainers=fetch,
                nodeSelector={"cloud.google.com/gke-nodepool": pool}, tolerations=tolerations,
                labels={"app": "spark-replay-validation", "run": name})


conf = {
    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark-admin-service-account",
    "spark.driver.extraClassPath": "/opt/replay/*:/opt/extra/*",
    "spark.executor.extraClassPath": "/opt/extra/*",
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": BUCKET + "/eventlogs",
    "spark.dynamicAllocation.enabled": "false",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "400",
}
if mode == "run":
    conf["spark.extraListeners"] = "org.apache.spark.sql.replay.ReplayCaptureListener"
else:
    # The event log directory does not exist before the first gen run has created it.
    conf["spark.eventLog.enabled"] = "false"
conf.update(extra)

driver = pod("regnp32driver1", cores=2, memory="8g")
driver["volumeMounts"] = [{"name": "replay", "mountPath": "/opt/replay"},
                          {"name": "extra", "mountPath": "/opt/extra"}]
executor = pod("spnp32sparknp1", instances=int(os.environ.get("REPLAY_EXECUTORS", "3")),
               cores=int(os.environ.get("REPLAY_EXECUTOR_CORES", "5")),
               memory=os.environ.get("REPLAY_EXECUTOR_MEMORY", "24g"))
executor["volumeMounts"] = [{"name": "extra", "mountPath": "/opt/extra"}]

print(json.dumps({
    "apiVersion": "sparkoperator.k8s.io/v1beta2", "kind": "SparkApplication",
    "metadata": {"name": "spark-replay-" + name, "namespace": "spark-operator"},
    "spec": {
        "type": "Python", "pythonVersion": "3", "mode": "cluster", "sparkVersion": "3.5.8",
        "image": IMAGE, "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "local:///opt/replay/replay_validation.py",
        "arguments": [mode, DATA] +
                     ([SCALE, conf["spark.eventLog.dir"]] if mode == "gen" else []),
        "restartPolicy": {"type": "Never"}, "timeToLiveSeconds": 172800,
        "volumes": [{"name": "replay", "configMap": {"name": "spark-replay-app"}},
                    {"name": "extra", "emptyDir": {}}],
        "sparkConf": conf, "driver": driver, "executor": executor,
    },
}, indent=1))
