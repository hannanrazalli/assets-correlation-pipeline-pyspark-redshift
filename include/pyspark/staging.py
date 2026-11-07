import os
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds"])
job_name = args["JOB_NAME"]
date_str = args["ds"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET_NAME = os.getenv("BUCKET_NAME")

year, month, day = date_str.split("-")
raw_forex_path = f"s3://raw/forex/yea={year}/month={month}/day={day}/"
raw_stocks_path = f"s3://raw/stocks/yea={year}/month={month}/day={day}/"
raw_gold_path = f"s3://raw/gold/yea={year}/month={month}/day={day}/"
raw_bitcoin_path = f"s3://raw/bitcoin/yea={year}/month={month}/day={day}/"

df_raw_fx = spark.read.json(raw_forex_path)
df_raw_stocks = spark.read.parquet(raw_stocks_path)
df_raw_gold = spark.read.parquet(raw_gold_path)
df_raw_btc = spark.read.parquet(raw_bitcoin_path)
