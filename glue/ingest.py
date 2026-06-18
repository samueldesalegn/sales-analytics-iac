"""Glue ETL: raw CSV landing -> curated Parquet (partitioned, dashboard-ready).

Reads --RAW_BUCKET / --CURATED_BUCKET from job args (set in template.yaml),
typecasts the raw fields, derives the measures a BI layer needs
(revenue, cost, profit) from units/unit_price/unit_cost, partitions by
year/month from sale_date, and writes Snappy Parquet to the curated zone
that the `sales_curated` external table reads.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "CURATED_BUCKET"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

raw_path = f"s3://{args['RAW_BUCKET']}/sales/"
curated_path = f"s3://{args['CURATED_BUCKET']}/sales/"

# 1. read raw CSV
raw = (
    spark.read
    .option("header", True)
    .option("mode", "DROPMALFORMED")
    .csv(raw_path)
)

# 2. clean / typecast the raw columns
clean = (
    raw
    .withColumn("sale_id", F.col("sale_id").cast("string"))
    .withColumn("sale_date", F.to_date(F.col("sale_date")))
    .withColumn("region", F.trim(F.col("region")))
    .withColumn("channel", F.trim(F.col("channel")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("product", F.trim(F.col("product")))
    .withColumn("customer_segment", F.trim(F.col("customer_segment")))
    .withColumn("units", F.col("units").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("unit_cost", F.col("unit_cost").cast("double"))
    .where(F.col("sale_id").isNotNull() & F.col("sale_date").isNotNull())
)

# 3. derive the measures the dashboards compare
enriched = (
    clean
    .withColumn("revenue", F.round(F.col("units") * F.col("unit_price"), 2))
    .withColumn("cost",    F.round(F.col("units") * F.col("unit_cost"), 2))
    .withColumn("profit",  F.round(F.col("units") * (F.col("unit_price") - F.col("unit_cost")), 2))
)

# 4. derive partition columns from sale_date (enables Athena partition pruning)
partitioned = (
    enriched
    .withColumn("load_year", F.year("sale_date").cast("string"))
    .withColumn("load_month", F.lpad(F.month("sale_date").cast("string"), 2, "0"))
)

# 5. write Snappy Parquet, partitioned, to the curated zone
(
    partitioned.write
    .mode("overwrite")
    .partitionBy("load_year", "load_month")
    .parquet(curated_path)
)

job.commit()
