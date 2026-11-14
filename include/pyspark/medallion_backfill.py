import sys
from functools import reduce

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds"])

job_name = args["JOB_NAME"]
target_date = args["ds"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def safe_read(spark, path, file_format="parquet"):
    try:
        if file_format == "json":
            return spark.read.json(path)
        return spark.read.parquet(path)

    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            print(f"[INFO] Folder not found: {e}")
            return None
        raise e


def process_forex(df_raw_fx):
    if df_raw_fx is None:
        return None

    df_stg_forex = df_raw_fx.select(
        F.to_date("date").alias("forex_date"),
        F.col("base").cast(T.StringType()).alias("base_currency"),
        F.col("quote").cast(T.StringType()).alias("target_currency"),
        F.col("rate").try_cast(T.DecimalType(18, 6)).alias("exchange_rate"),
        F.current_timestamp().alias("staged_at"),
    ).filter(
        F.col("forex_date").isNotNull()
        & F.col("base_currency").isNotNull()
        & F.col("target_currency").isNotNull()
        & F.col("exchange_rate").isNotNull()
    )

    window_forex = Window.partitionBy(
        "forex_date", "base_currency", "target_currency"
    ).orderBy(F.col("staged_at").desc())

    df_int_fx = (
        df_stg_forex.withColumn("rn", F.row_number().over(window_forex))
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

    return df_int_fx


def process_assets(df_raw_stocks, df_raw_btc, df_raw_gold):
    df_assets = []

    if df_raw_stocks is not None:
        df_stg_gspc = df_raw_stocks.select(
            F.to_date("Date").alias("price_date"),
            F.lit("GSPC").cast(T.StringType()).alias("ticker_symbol"),
            F.col("GSPC").try_cast(T.DecimalType(18, 6)).alias("close_price"),
        )
        df_stg_klse = df_raw_stocks.select(
            F.to_date("Date").alias("price_date"),
            F.lit("KLSE").cast(T.StringType()).alias("ticker_symbol"),
            F.col("KLSE").try_cast(T.DecimalType(18, 6)).alias("close_price"),
        )

        df_assets.extend([df_stg_gspc, df_stg_klse])

    if df_raw_btc is not None:
        df_stg_btc = df_raw_btc.select(
            F.to_date("Date").alias("price_date"),
            F.lit("BTC_USD").cast(T.StringType()).alias("ticker_symbol"),
            F.col("BTC-USD").try_cast(T.DecimalType(18, 6)).alias("close_price"),
        )

        df_assets.append(df_stg_btc)

    if df_raw_gold is not None:
        df_stg_gold = df_raw_gold.select(
            F.to_date("Date").alias("price_date"),
            F.lit("GLD").cast(T.StringType()).alias("ticker_symbol"),
            F.col("GLD").try_cast(T.DecimalType(18, 6)).alias("close_price"),
        )

        df_assets.append(df_stg_gold)

    if not df_assets:
        return None

    df_stg_assets = (
        reduce(DataFrame.unionByName, df_assets)
        .withColumn("staged_at", F.current_timestamp())
        .filter(
            F.col("price_date").isNotNull()
            & F.col("ticker_symbol").isNotNull()
            & F.col("close_price").isNotNull()
        )
    )

    window_assets = Window.partitionBy("price_date", "ticker_symbol").orderBy(
        F.col("staged_at").desc()
    )

    df_int_assets = (
        df_stg_assets.withColumn("rn", F.row_number().over(window_assets))
        .filter(F.col("rn") == 1)
        .select(
            F.md5(
                F.concat_ws(
                    "||",
                    F.col("price_date").cast("string"),
                    F.col("ticker_symbol"),
                )
            ).alias("assets_key"),
            F.col("price_date"),
            F.col("ticker_symbol"),
            F.col("close_price"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    return df_int_assets


BUCKET = "asset-correlation-pipeline-212105053682-ap-southeast-1-an"

year, month, day = target_date.split("-")
forex_raw_path = f"s3://{BUCKET}/raw/forex/year={year}/month={month}/day={day}/"
stocks_raw_path = f"s3://{BUCKET}/raw/stocks/year={year}/month={month}/day={day}/"
bitcoin_raw_path = f"s3://{BUCKET}/raw/bitcoin/year={year}/month={month}/day={day}/"
gold_raw_path = f"s3://{BUCKET}/raw/gold/year={year}/month={month}/day={day}/"

forex_silver_path = f"s3://{BUCKET}/pyspark/forex/year={year}/month={month}/day={day}/"
assets_silver_path = (
    f"s3://{BUCKET}/pyspark/assets/year={year}/month={month}/day={day}/"
)

df_raw_fx = safe_read(spark, forex_raw_path, file_format="json")
df_raw_stocks = safe_read(spark, stocks_raw_path)
df_raw_bitcoin = safe_read(spark, bitcoin_raw_path)
df_raw_gold = safe_read(spark, gold_raw_path)

df_int_forex = process_forex(df_raw_fx)
df_int_assets = process_assets(df_raw_stocks, df_raw_bitcoin, df_raw_gold)

if df_int_forex is not None:
    df_int_forex.write.mode("overwrite").parquet(forex_silver_path)

if df_int_assets is not None:
    df_int_assets.write.mode("overwrite").parquet(assets_silver_path)

job.commit()
