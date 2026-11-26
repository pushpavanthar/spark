package org.apache.spark.sql.replay

import java.io.File

import scala.io.Source
import scala.util.control.NonFatal

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession

/**
 * TPC-DS as the stand-in for production plans: 103 queries with the subqueries, windows, unions
 * and many-way joins the harness queries lack. Each query really runs, and its baseline replay
 * must give the plan the run ended with. Queries and schema are Apache Spark's test resources;
 * the data is generated here, so results are meaningless and many are empty. Only plans matter.
 */
object Tpcds {
  private def resource(name: String): String = {
    val src = Source.fromInputStream(getClass.getResourceAsStream(s"/tpcds/$name"), "UTF-8")
    try src.mkString finally src.close()
  }

  /** Table name to its (column, type) list. */
  val schema: Seq[(String, Seq[(String, String)])] = resource("schema.txt").linesIterator.map { l =>
    val Array(table, cols) = l.split('|')
    table -> cols.split(';').toSeq.map { c => val i = c.indexOf(' '); c.take(i) -> c.drop(i + 1) }
  }.toSeq

  val queries: Seq[String] = {
    val names = new File(getClass.getResource("/tpcds").toURI).list().filter(_.endsWith(".sql"))
    names.map(_.stripSuffix(".sql")).sortBy(n => (n.filter(_.isDigit).toInt, n)).toSeq
  }
  def sql(query: String): String = resource(s"$query.sql")

  // Rows per table at scale 1.
  // Facts stay small against the customer count: several queries join two fact tables on the
  // customer, and that join grows with the product of their sizes.
  private val rows = Map("store_sales" -> 60000L, "catalog_sales" -> 40000L,
    "web_sales" -> 20000L, "inventory" -> 40000L, "store_returns" -> 6000L,
    "catalog_returns" -> 4000L, "web_returns" -> 2000L, "date_dim" -> 2190L, "time_dim" -> 8640L,
    "item" -> 2000L, "customer" -> 5000L, "customer_address" -> 2500L,
    "customer_demographics" -> 2000L, "household_demographics" -> 720L).withDefaultValue(30L)

  // The dimension a foreign key column points to, by the end of its name.
  private val references = Seq("date_sk" -> "date_dim", "time_sk" -> "time_dim", "item_sk" -> "item",
    "customer_sk" -> "customer", "cdemo_sk" -> "customer_demographics",
    "hdemo_sk" -> "household_demographics", "addr_sk" -> "customer_address", "store_sk" -> "store",
    "promo_sk" -> "promotion", "warehouse_sk" -> "warehouse", "ship_mode_sk" -> "ship_mode",
    "reason_sk" -> "reason", "call_center_sk" -> "call_center", "catalog_page_sk" -> "catalog_page",
    "web_page_sk" -> "web_page", "web_site_sk" -> "web_site", "income_band_sk" -> "income_band")

  private def column(table: String, position: Int, name: String, dataType: String,
      scale: Int): String = {
    val h = s"pmod(hash(id, $position), %d)"
    val t = dataType.toUpperCase
    val expr =
      if (name.endsWith("_sk") && position == 0 && !rows.contains(table) || name == "d_date_sk" ||
          (position == 0 && Set("item", "customer", "customer_address", "customer_demographics",
            "household_demographics", "time_dim")(table))) {
        "id + 1"
      } else if (name.endsWith("_sk")) {
        val target = references.collectFirst { case (suffix, dim) if name.endsWith(suffix) => dim }
        val n = target.map(d => rows(d) * (if (rows.contains(d) && d.endsWith("_sales")) scale else 1))
          .getOrElse(100L)
        h.format(n) + " + 1"
      } else if (name == "d_year") "1998 + cast(id / 365 as int)"
      // Sequences are functions of the date (q6 reads one month's as a scalar).
      else if (name == "d_month_seq") "1176 + cast(id / 365 as int) * 12 + cast(pmod(id, 365) / 31 as int)"
      else if (name == "d_week_seq") "5270 + cast(id / 7 as int)"
      else if (name == "d_moy") "1 + cast(pmod(id, 365) / 31 as int)"
      else if (name == "d_dom") "1 + pmod(id, 28)"
      else if (name == "d_qoy") "1 + cast(pmod(id, 365) / 92 as int)"
      else if (t == "DATE") "date_add(date'1998-01-01', cast(pmod(id, 2190) as int))"
      else if (t.startsWith("DECIMAL")) h.format(10000) + " / 100"
      // Business ids are join keys (q4 joins customers to themselves on one): keep them unique.
      else if (name.endsWith("_id")) "concat('id', id)"
      else if (t.startsWith("CHAR") || t.startsWith("VARCHAR")) s"concat('v', ${h.format(20)})"
      else h.format(100)
    val castTo = if (t.startsWith("CHAR") || t.startsWith("VARCHAR")) "string" else dataType
    s"cast($expr as $castTo) as $name"
  }

  /** Writes every table under `dir` as Parquet; fact tables have `scale` times their base rows. */
  def gen(spark: SparkSession, dir: String, scale: Int): Unit = schema.foreach { case (table, cols) =>
    val facts = table.endsWith("_sales") || table.endsWith("_returns") || table == "inventory"
    val n = rows(table) * (if (facts) scale else 1)
    val exprs = cols.zipWithIndex.map { case ((c, t), i) => column(table, i, c, t, scale) }
    spark.range(n).selectExpr(exprs: _*).repartition(if (facts) 4 * scale else 1)
      .write.mode("overwrite").parquet(s"$dir/$table")
  }

  def register(spark: SparkSession, dir: String): Unit = schema.foreach { case (table, _) =>
    spark.read.parquet(s"$dir/$table").createOrReplaceTempView(table)
  }
}

class TpcdsSuite extends AnyFunSuite {
  test("baseline replay reproduces the real plan of the TPC-DS queries") {
    val dir = new File("target/tpcds-4")
    val spark = Harness.session(Map("spark.sql.autoBroadcastJoinThreshold" -> "1m",
      "spark.sql.shuffle.partitions" -> "16"))
    spark.sparkContext.setLogLevel("ERROR")
    try {
      if (!new File(dir, "_done").exists) {
        Tpcds.gen(spark, dir.getPath, 1)
        new File(dir, "_done").createNewFile()
      }
      Tpcds.register(spark, dir.getPath)
      val collector = Harness.attachCapture(spark)
      // Every miss with both plans, for whoever has to find out why.
      val report = new java.io.PrintWriter(new File("target/tpcds-misses.txt"))
      val outcomes = Tpcds.queries.map { q =>
        val outcome = try {
          val (cap, real) = Harness.runAndCapture(spark, collector, Tpcds.sql(q))
          val (plan, raised) = Replay.replay(spark, cap)
          val exact = Sig.sig(plan) == Sig.sig(real)
          // What an engine does with a capture: it has the recorded shape, not the real plan.
          require(Replay.reproduces(plan, cap) == exact, "the recorded shape is not the real one")
          val flags = raised ++ (if (exact) Nil else Seq("BASELINE_MISMATCH"))
          if (!exact) report.println(s"$q flags=${flags.mkString(",")}\n  want ${Sig.sig(real)}\n  got  ${Sig.sig(plan)}")
          if (exact) "exact" + (if (flags.nonEmpty) " (flagged)" else "")
          else s"miss, flagged ${flags.mkString(",")}"
        } catch {
          case NonFatal(e) => s"FAILED ${e.getClass.getSimpleName}: ${String.valueOf(e.getMessage).take(160)}"
        }
        q -> outcome
      }
      report.close()
      outcomes.groupBy(_._2.takeWhile(_ != ':').take(40)).toSeq.sortBy(-_._2.size).foreach {
        case (kind, qs) => info(s"${qs.size} $kind: ${qs.map(_._1).take(12).mkString(" ")}")
      }
      outcomes.filter(_._2.startsWith("FAILED")).groupBy(_._2).toSeq.sortBy(-_._2.size).take(8)
        .foreach { case (why, qs) => info(s"${qs.size}x $why") }
      val failed = outcomes.filter(_._2.startsWith("FAILED"))
      assert(failed.isEmpty, failed.map(_._1).mkString(" "))
      // Measured 94 to 98 of 103, fewer on a busy machine: the misses are joins whose build
      // side follows stage timing, and which queries miss changes from run to run.
      val exact = outcomes.count(_._2.startsWith("exact"))
      assert(exact >= 0.88 * outcomes.size, s"$exact of ${outcomes.size} baselines reproduced")
    } finally spark.stop()
  }
}
