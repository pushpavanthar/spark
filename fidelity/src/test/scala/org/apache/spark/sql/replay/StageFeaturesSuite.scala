package org.apache.spark.sql.replay

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.funsuite.AnyFunSuite

/**
 * Writes the stage features of every event log `$REPLAY_FEATURE_LOGS/<name>/<event log>` to
 * `target/features/<name>.json`. Canceled without that directory.
 */
class StageFeaturesSuite extends AnyFunSuite {
  test("stage features of cluster event logs") {
    val root = sys.env.get("REPLAY_FEATURE_LOGS").map(new File(_))
    assume(root.isDefined, "REPLAY_FEATURE_LOGS is not set")
    val out = new File("target/features")
    out.mkdirs()
    val spark = Harness.session()
    try {
      root.get.listFiles().filter(_.isDirectory).foreach { run =>
        val rows = StageFeatures.extract(spark, run.listFiles().head.getPath)
        new ObjectMapper().registerModule(DefaultScalaModule)
          .writeValue(new File(out, run.getName + ".json"), rows)
        info(s"${run.getName}: ${rows.size} stages described")
        assert(rows.nonEmpty)
      }
    } finally spark.stop()
  }
}
