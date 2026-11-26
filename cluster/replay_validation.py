"""Self-contained workload for validating capture and replay on a cluster.

  replay_validation.py gen <dir> <scale>   writes the synthetic tables (scale 1 = 4M sales rows)
  replay_validation.py run <dir>           runs the queries; SQL conf comes from the submission

The tables and queries are those of the local fidelity harness (Harness.scala), so results on a
cluster compare with the local ones. Reads and writes nothing but <dir>.
"""
import sys

from pyspark.sql import SparkSession

QUERIES = {
    "q1_dims": """select i.category, sum(s.price) from sales s join item i on s.item_sk = i.item_sk
      join dates d on s.date_sk = d.date_sk where d.year = 2001 group by i.category""",
    "q2_filtered_dim": """select c.state, count(*) from sales s join cust c
      on s.cust_sk = c.cust_sk where c.segment = 7 group by c.state""",
    "q3_skew": """select v.wh, sum(s.qty) from sales s join inv v on s.item_sk = v.item_sk
      group by v.wh""",
    "q4_agg_join": """select c.state, sum(t.tot) from (select cust_sk, sum(price) tot from sales
      group by cust_sk) t join cust c on t.cust_sk = c.cust_sk group by c.state""",
    "q5_empty": """select i.brand, count(*) from sales s join item i on s.item_sk = i.item_sk
      where i.category = -1 group by i.brand""",
    "q6_skew_dims": """select i.brand, v.wh, sum(s.qty) from sales s
      join inv v on s.item_sk = v.item_sk join item i on s.item_sk = i.item_sk
      where i.category = 3 group by i.brand, v.wh""",
    "q7_byte_skew": """select v.wh, max(s.pad), sum(s.qty) from sales s join inv v
      on s.item_sk = v.item_sk group by v.wh""",
    "q8_byte_skew_dim": """select i.category, v.wh, max(s.pad) from sales s
      join inv v on s.item_sk = v.item_sk join item i on s.item_sk = i.item_sk
      group by i.category, v.wh""",
    "q9_clustered_skew": """select v.wh, max(s.pad), sum(s.qty) from sales_c s join inv v
      on s.item_sk = v.item_sk group by v.wh""",
}
TABLES = ["sales", "sales_c", "item", "cust", "dates", "inv"]


def gen(spark, out, scale):
    n = 4000000 * scale
    files = 8 * scale
    # One sales row in five has item_sk 1: the skew the queries are about.
    spark.range(n).selectExpr(
        "case when id % 5 = 0 then 1 else (id * 7919) % 200000 end as item_sk",
        "(id * 104729) % 500000 as cust_sk", "id % 366 as date_sk",
        "cast(id % 10 as int) as qty", "cast((id % 1000) / 10.0 as double) as price",
        "sha2(cast(id as string), 256) as pad",
    ).repartition(files).write.mode("overwrite").parquet(out + "/sales")
    # The same rows clustered by key: the hot item_sk sits in few files, so mappers are uneven.
    spark.read.parquet(out + "/sales").repartitionByRange(files, "item_sk") \
        .write.mode("overwrite").parquet(out + "/sales_c")
    spark.range(200000).selectExpr(
        "id as item_sk", "cast(id % 50 as int) as category", "concat('brand_', id % 500) as brand",
    ).repartition(2).write.mode("overwrite").parquet(out + "/item")
    spark.range(500000).selectExpr(
        "id as cust_sk", "concat('st_', id % 50) as state", "cast(id % 100 as int) as segment",
    ).repartition(2).write.mode("overwrite").parquet(out + "/cust")
    spark.range(366).selectExpr("id as date_sk", "cast(2000 + id % 3 as int) as year") \
        .repartition(1).write.mode("overwrite").parquet(out + "/dates")
    # inv has four rows per item, so the hot item multiplies by four in the joins.
    spark.range(800000).selectExpr(
        "id % 200000 as item_sk", "cast(id % 4 as int) as wh", "cast(id % 100 as int) as onhand",
    ).repartition(2).write.mode("overwrite").parquet(out + "/inv")


def run(spark, data):
    for t in TABLES:
        spark.read.parquet(data + "/" + t).createOrReplaceTempView(t)
    for name, sql in QUERIES.items():
        spark.sparkContext.setJobDescription(name)
        spark.sql(sql).collect()


if __name__ == "__main__":
    mode, path = sys.argv[1], sys.argv[2]
    session = SparkSession.builder.getOrCreate()
    if mode == "gen":
        gen(session, path, int(sys.argv[3]))
    elif mode == "run":
        run(session, path)
    else:
        raise SystemExit("usage: replay_validation.py gen <dir> <scale> | run <dir>")
    session.stop()
