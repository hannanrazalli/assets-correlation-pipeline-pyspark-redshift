import os
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds"])
job_name = args["JOB_NAME"]
date_str = args["ds"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

year, month, day = date_str.split("-")

BUCKET_NAME = os.getenv("BUCKET_NAME")
forex_path = f"s3://{BUCKET_NAME}/raw/forex/year={year}/month={month}/day={day}/"
stocks_path = f"s3://{BUCKET_NAME}/raw/stocks/year={year}/month={month}/day={day}/"
bitcoin_path = f"s3://{BUCKET_NAME}/raw/bitcoin/year={year}/month={month}/day={day}/"
gold_path = f"s3://{BUCKET_NAME}/raw/gold/year={year}/month={month}/day={day}/"

# --- 1. READ FILES ---
df_raw_fx = spark.read.json(forex_path)
df_raw_stocks = spark.read.parquet(stocks_path)
df_raw_btc = spark.read.parquet(bitcoin_path)
df_raw_gold = spark.read.parquet(gold_path)


# --- 2. SILVER - RENAMED & CASTED ---
df_stg_fx = df_raw_fx.select(
    F.to_date(F.col("date")).alias("forex_date"),
    F.col("base").cast(T.StringType()).alias("base_currency"),
    F.col("quote").cast(T.StringType()).alias("target_currency"),
    F.col("rate").try_cast(T.DecimalType(18, 6)).alias("exchange_rate"),
).withColumn("staged_at", F.current_timestamp())

df_gspc = df_raw_stocks.filter(F.col("GSPC").isNotNull()).select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("GSPC").alias("ticker_symbol"),
    F.col("GSPC").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_klse = df_raw_stocks.filter(F.col("KLSE").isNotNull()).select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("KLSE").alias("ticker_symbol"),
    F.col("KLSE").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_stg_btc = df_raw_btc.filter(F.col("btc-usd").isNotNull()).select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("BTC_USD").alias("ticker_symbol"),
    F.col("btc-usd").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_stg_gold = df_raw_gold.filter(F.col("GLD").isNotNull()).select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("GLD").alias("ticker_symbol"),
    F.col("GLD").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_stg_assets = (
    df_gspc.unionByName(df_klse)
    .unionByName(df_stg_btc)
    .unionByName(df_stg_gold)
    .withColumn("staged_at", F.current_timestamp())
)

window_spec = Window.partitionBy("price_date", "ticker_symbol").orderBy(
    F.col("staged_at").desc()
)

df_int_assets = (
    df_stg_assets.withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .select(
        F.md5(
            F.concat_ws(
                "||", F.col("price_date").cast("string"), F.col("ticker_symbol")
            )
        ).alias("asset_key"),
        F.col("price_date"),
        F.col("ticker_symbol"),
        F.col("close_price"),
        F.current_timestamp().alias("processed_at"),
    )
)

forex_window_spec = Window.partitionBy(
    "forex_date", "base_currency", "target_currency"
).orderBy(F.col("staged_at").desc())

df_int_fx = (
    df_stg_fx.withColumn("rn", F.row_number().over(forex_window_spec))
    .filter(F.col("rn") == 1)
    .select(
        F.md5(
            F.concat_ws(
                "||",
                F.col("forex_date").cast("string"),
                F.col("base_currency"),
                F.col("target_currency"),
            )
        ).alias("forex_key"),
        F.col("forex_date"),
        F.col("base_currency"),
        F.col("target_currency"),
        F.col("exchange_rate"),
        F.current_timestamp().alias("processed_at"),
    )
)

silver_assets_path = f"s3://{BUCKET_NAME}/raw_pyspark/assets"
silver_forex_path = f"s3://{BUCKET_NAME}/raw_pyspark/forex"

(df_int_assets.write.mode("overwrite").parquet(silver_assets_path))

(df_int_fx.write.mode("overwrite").parquet(silver_forex_path))
