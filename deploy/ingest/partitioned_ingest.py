"""Distributed partitioned ingestion on Spark → Iceberg or Delta (atomic, idempotent MERGE).

spark-submit partitioned_ingest.py --help

Sources:
  jdbc   : PostgreSQL/MySQL. Logical partitions become Spark `predicates` (one task each), so reads are parallel and
           non-overlapping. Use the predicates exported from Meridian's planner.
  kafka  : topic partitions with explicit start/end offsets (checkpointed via Structured Streaming checkpointLocation).
  files  : S3 (s3a://) or ADLS (abfss://) Parquet/Avro/CSV/JSON; file lists bin-packed by Meridian's planner.
  rest   : page ranges fetched in mapPartitions.
Targets: iceberg (MERGE INTO catalog.db.table) or delta (MERGE via DeltaTable). One commit per run = atomic publish.
"""
import argparse, json

p = argparse.ArgumentParser()
p.add_argument("--source", choices=["jdbc", "kafka", "files", "rest"], required=True)
p.add_argument("--target-format", choices=["iceberg", "delta"], required=True)
p.add_argument("--target", required=True, help="iceberg: catalog.db.table | delta: s3a://bucket/path")
p.add_argument("--pk", required=True)
p.add_argument("--jdbc-url"); p.add_argument("--table"); p.add_argument("--user"); p.add_argument("--password")
p.add_argument("--predicates", help="JSON file: list of WHERE strings from the planner")
p.add_argument("--fetchsize", type=int, default=5000)
p.add_argument("--kafka-servers"); p.add_argument("--topic"); p.add_argument("--starting-offsets", default="earliest"); p.add_argument("--ending-offsets", default="latest")
p.add_argument("--checkpoint", help="checkpointLocation for streaming sources")
p.add_argument("--path"); p.add_argument("--format", default="parquet")
p.add_argument("--rest-url"); p.add_argument("--pages", type=int, default=0)
p.add_argument("--spark-partitions", type=int, default=0, help="target-side write partitions (0 = keep)")
a = p.parse_args()

from pyspark.sql import SparkSession  # noqa: E402
spark = SparkSession.builder.appName("meridian-partitioned-ingest").getOrCreate()

if a.source == "jdbc":
    preds = json.load(open(a.predicates))
    df = spark.read.jdbc(a.jdbc_url, a.table, predicates=preds,
                         properties={"user": a.user or "", "password": a.password or "", "fetchsize": str(a.fetchsize)})
elif a.source == "kafka":
    df = (spark.read.format("kafka").option("kafka.bootstrap.servers", a.kafka_servers).option("subscribe", a.topic)
          .option("startingOffsets", a.starting_offsets).option("endingOffsets", a.ending_offsets).load()
          .selectExpr("CAST(key AS STRING) AS k", "CAST(value AS STRING) AS v", "partition", "offset"))
    df = spark.read.json(df.rdd.map(lambda r: r.v))
elif a.source == "files":
    df = spark.read.format(a.format).load(a.path)
else:
    import urllib.request
    def fetch(pages):
        for pg in pages:
            with urllib.request.urlopen(f"{a.rest_url}{'&' if '?' in a.rest_url else '?'}page={pg}", timeout=30) as r:
                for row in json.loads(r.read()):
                    yield json.dumps(row)
    df = spark.read.json(spark.sparkContext.parallelize(range(1, a.pages + 1), max(1, a.pages // 10)).mapPartitions(fetch))

df = df.dropDuplicates([a.pk])
if a.spark_partitions:
    df = df.repartition(a.spark_partitions, a.pk)
df.createOrReplaceTempView("incoming")

if a.target_format == "iceberg":
    cols = df.columns
    if not spark.catalog.tableExists(a.target):
        df.writeTo(a.target).using("iceberg").create()
    else:
        sets = ", ".join(f"t.`{c}` = s.`{c}`" for c in cols)
        spark.sql(f"MERGE INTO {a.target} t USING incoming s ON t.`{a.pk}` = s.`{a.pk}` "
                  f"WHEN MATCHED THEN UPDATE SET {sets} WHEN NOT MATCHED THEN INSERT *")
else:
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, a.target):
        (DeltaTable.forPath(spark, a.target).alias("t").merge(df.alias("s"), f"t.`{a.pk}` = s.`{a.pk}`")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        df.write.format("delta").save(a.target)

print(json.dumps({"rows": df.count(), "target": a.target}))
