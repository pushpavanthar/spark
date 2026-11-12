package org.apache.spark.sql.replay

import java.io.File

import org.scalatest.funsuite.AnyFunSuite

/**
 * The gate every engine or capture change must pass. Data and ground truth are generated on first
 * run (about 3 minutes) and cached under target/fidelity/<harness version>.
 */
class FidelityGateSuite extends AnyFunSuite {
  // Run-to-run noise is one or two flagged pairs (which stages AQE cancels varies), hence the
  // slack below the measured 0.93. Unflagged misses have no slack: a wrong answer must say so.
  private val minExactRate = 0.90

  test("replay predicts the final plan of real runs under changed configs") {
    val spark = Harness.session()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val dir = Harness.prepare(spark)
      val detail = new File(dir, "last_eval.txt")
      val collector = Harness.attachCapture(spark)
      val score = Harness.eval(spark, collector, Harness.loadTruth(new File(dir, "truth.tsv")), detail)
      info(score.toString)
      assert(score.unflaggedMisses == 0, s"wrong predictions without a flag; see $detail")
      assert(score.exactRate >= minExactRate, s"see $detail")
    } finally spark.stop()
  }
}
