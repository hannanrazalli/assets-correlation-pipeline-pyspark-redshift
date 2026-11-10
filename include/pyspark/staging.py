import sys
import os

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, types as T
from pyspark.sql.types import DecimalType, StringType


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

df_stg_fx = (
    df_raw_fx.select(
        F.to_date(F.col("date")).alias("forex_date"),
        F.col("base").cast(T.StringType()).alias("base_currency"),
        F.col("quote").cast(T.StringType()).alias("target_currency"),
        F.col("rate").try_cast(T.DecimalType(18, 6)).alias("exchange_rate")
    )
    .withColumn("staged_at", F.current_timestamp())
    .filter(F.col("forex_date").isNotNull() & F.col("exchange_rate").isNotNull())
)

df_gspc = (
    df_raw_stocks.select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("GSPC").cast(T.StringType()).alias("ticker_symbol"),
        F.col("GSPC").try_cast(T.Decimal_Type(18, 6)).alias("close_price")
    )
)

df_klse = (
    df_raw_stocks.select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("KLSE").cast(T.StringType()).alias("ticker_symbol"),
        F.col("KLSE").try_cast(T.Decimal_Type(18, 6)).alias("close_price")
    )
)

df_stg_stocks = (
    df_gspc
    .unionByName(df_klse)
    .withColumn("staged_at", F.current_timestamp()))

df_stg_fx.show(5, truncate=False)
df_stg_stocks.show(5, truncate=False)


###################

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

BUCKET="asset-correlation-pipeline-212105053682-ap-southeast-1-an"
fx_raw_path = f"s3://{BUCKET}/raw/forex/"         
stocks_raw_path = f"s3://{BUCKET}/raw/stocks/"

df_raw_fx = spark.read.json(fx_raw_path)
df_raw_stocks = spark.read.parquet(stocks_raw_path)

df_stg_fx = (
    df_raw_fx.select(
        F.to_date(F.col("date")).alias("forex_date"),
        F.col("base").cast(T.StringType()).alias("base_currency"),
        F.col("quote").cast(T.StringType()).alias("target_currency"),
        F.col("rate").try_cast(T.DecimalType(18, 6)).alias("exchange_rate")
    )
    .withColumn("staged_at", F.current_timestamp())
    .filter(F.col("forex_date").isNotNull() & F.col("exchange_rate").isNotNull())
)

df_gspc = (
    df_raw_stocks.select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("GSPC").cast(T.StringType()).alias("ticker_symbol"),
        F.col("GSPC").try_cast(T.DecimalType(18, 6)).alias("close_price")
    )
)

df_klse = (
    df_raw_stocks.select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("KLSE").cast(T.StringType()).alias("ticker_symbol"),
        F.col("KLSE").try_cast(T.DecimalType(18, 6)).alias("close_price")
    )
)

df_stg_stocks = (
    df_gspc
    .unionByName(df_klse)
    .withColumn("staged_at", F.current_timestamp()))


window_fx = (
    Window
    .partitionBy("forex_date", "base_currency", "target_currency")
    .orderBy(F.col("staged_at"))
)

window_stocks = (
    Window
    .partitionBy("price_date", "ticker_symbol")
    .orderBy(F.col("staged_at"))
)

df_int_fx = (
    df_stg_fx
        .withColumn("rn", F.row_number().over(window_fx))
        .filter(F.col("rn")==1)
        .select(
            F.col("forex_date"),
            F.col("base_currency"),
            F.col("target_currency"),
            F.col("exchange_rate")
        )
        .withColumn("processed_at", F.current_timestamp())
)

df_int_stocks = (
    df_stg_stocks
        .withColumn("rn", F.row_number().over(window_stocks))
        .filter(F.col("rn")==1)
        .select(
            F.col("price_date"),
            F.col("ticker_symbol"),
            F.col("close_price")
        )
        .withColumn("processed_at", F.current_timestamp())
)

fx_silver_path = f"s3://{BUCKET}/raw/pyspark/forex/"
stocks_silver_path = f"s3://{BUCKET}/raw/pyspark/stocks/"

df_int_fx.write.mode("overwrite").parquet(fx_silver_path)
.write.mode("overwrite").parquet(stocks_silver_path)