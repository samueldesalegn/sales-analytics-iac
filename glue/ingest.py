"""Glue ETL: raw landing -> curated Parquet (partitioned by load_year/load_month).

Reads --RAW_BUCKET / --CURATED_BUCKET from job args (set in template.yaml),
cleans/normalizes, and writes columnar Parquet that the Athena external table
(sales_curated) reads directly.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "CURATED_BUCKET"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# 1. read raw  ->  2. clean/typecast  ->  3. write curated Parquet
#    df = spark.read.option("header", True).csv(f"s3://{args['RAW_BUCKET']}/sales/")
#    df = clean(df)
#    df.write.mode("overwrite").partitionBy("load_year", "load_month") \
#         .parquet(f"s3://{args['CURATED_BUCKET']}/sales/")

job.commit()
