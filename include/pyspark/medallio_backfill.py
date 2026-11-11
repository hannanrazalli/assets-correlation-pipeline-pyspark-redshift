import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

job_name = args["JOB_NAME"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "asset-correlation-pipeline-212105053682-ap-southeast-1-an"

fx_raw_path = f"s3://{BUCKET}/raw/forex/"
stocks_raw_path = f"s3://{BUCKET}/raw/stocks/"
bitcoin_raw_path = f"s3://{BUCKET}/raw/bitcoin/"
gold_raw_path = f"s3://{BUCKET}/raw/gold/"

df_raw_fx = spark.read.json(fx_raw_path)
df_raw_stocks = spark.read.parquet(stocks_raw_path)
df_raw_btc = spark.read.parquet(bitcoin_raw_path)
df_raw_gold = spark.read.parquet(gold_raw_path)

df_stg_fx = (
    df_raw_fx.select(
        F.to_date(F.col("date")).alias("forex_date"),
        F.col("base").cast(T.StringType()).alias("base_currency"),
        F.col("quote").cast(T.StringType()).alias("target_currency"),
        F.col("rate").try_cast(T.DecimalType(18, 6)).alias("exchange_rate"),
    )
    .filter(F.col("base_currency").isNotNull() & F.col("exchange_rate").isNotNull())
    .withColumn("staged_at", F.current_timestamp())
)

df_stg_gspc = df_raw_stocks.select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("GSPC").alias("ticker_symbol"),
    F.col("GSPC").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_stg_klse = df_raw_stocks.select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("KLSE").alias("ticker_symbol"),
    F.col("KLSE").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_stg_btc = df_raw_btc.select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("BTC_USD").alias("ticker_symbol"),
    F.col("BTC-USD").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_stg_gld = df_raw_gold.select(
    F.to_date(F.col("Date")).alias("price_date"),
    F.lit("GLD").alias("ticker_symbol"),
    F.col("GLD").try_cast(T.DecimalType(18, 6)).alias("close_price"),
)

df_stg_assets = (
    df_stg_gspc.unionByName(df_stg_klse)
    .unionByName(df_stg_btc)
    .unionByName(df_stg_gld)
    .withColumn("staged_at", F.current_timestamp())
)

window_fx = Window.partitionBy(
    "forex_date", "base_currency", "target_currency"
).orderBy("staged_at")

window_assets = Window.partitionBy("price_date", "ticker_symbol").orderBy("staged_at")

df_int_fx = (
    df_stg_fx.withColumn("rn", F.row_number().over(window_fx))
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

df_int_assets = (
    df_stg_assets.withColumn("rn", F.row_number().over(window_assets))
    .filter(F.col("rn") == 1)
    .select(
        F.md5(
            F.concat_ws(
                "||",
                F.col("price_date"),
                F.col("ticker_symbol"),
            )
        ).alias("assets_key"),
        F.col("price_date"),
        F.col("ticker_symbol"),
        F.col("close_price"),
        F.current_timestamp().alias("processed_at"),
    )
)

fx_silver_path = f"s3://{BUCKET}/pyspark/forex/"
assets_silver_path = f"s3://{BUCKET}/pyspark/assets/"

(
    df_int_fx.withColumn("year", F.year("forex_date"))
    .withColumn("month", F.lpad(F.month("forex_date"), 2, "0"))
    .withColumn("day", F.lpad(F.dayofmonth("forex_date"), 2, "0"))
    .write.mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(fx_silver_path)
)

(
    df_int_assets.withColumn("year", F.year("price_date"))
    .withColumn("month", F.lpad(F.month("price_date"), 2, "0"))
    .withColumn("day", F.lpad(F.dayofmonth("price_date"), 2, "0"))
    .write.mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(assets_silver_path)
)

job.commit()
