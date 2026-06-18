"""Glue ETL: raw CSV landing -> curated Parquet (partitioned).

Reads --RAW_BUCKET / --CURATED_BUCKET from job args (set in template.yaml),
cleans and typecasts to the curated schema, derives year/month partition
columns from sale_date (so Athena can prune by date), and writes Snappy
Parquet to the curated zone that the `sales_curated` external table reads.
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

# 2. clean / typecast to the curated schema
clean = (
    raw
    .withColumn("sale_id", F.col("sale_id").cast("string"))
    .withColumn("sale_date", F.to_date(F.col("sale_date")))
    .withColumn("region", F.trim(F.col("region")))
    .withColumn("product", F.trim(F.col("product")))
    .withColumn("units", F.col("units").cast("int"))
    .withColumn("revenue", F.col("revenue").cast("double"))
    .where(F.col("sale_id").isNotNull() & F.col("sale_date").isNotNull())
)

# 3. derive partition columns from sale_date (enables Athena partition pruning)
partitioned = (
    clean
    .withColumn("load_year", F.year("sale_date").cast("string"))
    .withColumn("load_month", F.lpad(F.month("sale_date").cast("string"), 2, "0"))
)

# 4. write Snappy Parquet, partitioned, to the curated zone
(
    partitioned.write
    .mode("overwrite")
    .partitionBy("load_year", "load_month")
    .parquet(curated_path)
)

job.commit()
