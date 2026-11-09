import sys
import os

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

job_name = args["JOB_NAME"]
target_date = args["ds"]

BUCKET=os.getenv("BUCKET_NAME")

fx_raw_path = f"s3://{BUCKET}/raw/forex/"
stocks_raw_path = f"s3://{BUCKET}/raw/stocks/"

df_raw_fx = spark.read.json(fx_raw_path)
df_raw_stocks = spark.read.parquet(stocks_raw_path)

df_stg_fx = (df_raw_fx
             .select(
                 
             ))