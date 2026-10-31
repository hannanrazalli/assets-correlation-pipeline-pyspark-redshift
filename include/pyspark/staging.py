import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def get_spark_session(app_name: str = "AssetsStagingPipeline") -> SparkSession:
    """Mulakan atau guna SparkSession sedia ada."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def process_silver_forex(spark: SparkSession, bucket_name: str):
    """Transformasi dan deduplikasi Forex ke Silver."""
    forex_raw_path = f"s3://{bucket_name}/raw/forex/"
    forex_silver_path = f"s3://{bucket_name}/silver/forex/"

    df_raw = spark.read.json(forex_raw_path)

    # Staging
    df_stg = df_raw.select(
        F.to_date(F.col("date")).alias("forex_date"),
        F.col("base").cast(T.StringType()).alias("base_currency"),
        F.col("quote").cast(T.StringType()).alias("target_currency"),
        F.col("rate").try_cast(T.DecimalType(18, 6)).alias("exchange_rate"),
    ).withColumn("_staged_at", F.current_timestamp())

    # Intermediate / Deduplication
    window_spec = Window.partitionBy(
        "forex_date", "base_currency", "target_currency"
    ).orderBy(F.col("_staged_at").desc())

    df_int = (
        df_stg.withColumn("rn", F.row_number().over(window_spec))
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
            F.current_timestamp().alias("_intermediate_at"),
        )
    )

    # Simpan ke Silver Parquet
    df_int.coalesce(1).write.mode("overwrite").parquet(forex_silver_path)


def process_silver_assets(spark: SparkSession, bucket_name: str):
    """Transformasi, unifikasi 4 aset, dan deduplikasi ke Silver."""
    stocks_path = f"s3://{bucket_name}/raw/stocks/"
    btc_path = f"s3://{bucket_name}/raw/bitcoin/"
    gold_path = f"s3://{bucket_name}/raw/gold/"
    assets_silver_path = f"s3://{bucket_name}/silver/assets/"

    # 1. Stocks (GSPC & KLSE)
    df_raw_stocks = spark.read.parquet(stocks_path)
    df_gspc = df_raw_stocks.filter(F.col("GSPC").isNotNull()).select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("^GSPC").alias("ticker_symbol"),
        F.col("GSPC").try_cast(T.DecimalType(18, 6)).alias("close_price"),
    )
    df_klse = df_raw_stocks.filter(F.col("KLSE").isNotNull()).select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("^KLSE").alias("ticker_symbol"),
        F.col("KLSE").try_cast(T.DecimalType(18, 6)).alias("close_price"),
    )

    # 2. Bitcoin
    df_raw_btc = spark.read.parquet(btc_path)
    btc_col = "BTC-USD" if "BTC-USD" in df_raw_btc.columns else "Close"
    df_btc = df_raw_btc.filter(F.col(btc_col).isNotNull()).select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("BTC_USD").alias("ticker_symbol"),
        F.col(btc_col).try_cast(T.DecimalType(18, 6)).alias("close_price"),
    )

    # 3. Gold
    df_raw_gold = spark.read.parquet(gold_path)
    gold_col = "GLD" if "GLD" in df_raw_gold.columns else "Close"
    df_gold = df_raw_gold.filter(F.col(gold_col).isNotNull()).select(
        F.to_date(F.col("Date")).alias("price_date"),
        F.lit("GLD").alias("ticker_symbol"),
        F.col(gold_col).try_cast(T.DecimalType(18, 6)).alias("close_price"),
    )

    # 4. Union & Audit
    df_stg_assets = (
        df_gspc.unionByName(df_klse)
        .unionByName(df_btc)
        .unionByName(df_gold)
        .withColumn("_staged_at", F.current_timestamp())
    )

    # 5. Intermediate Deduplication
    window_spec = Window.partitionBy("price_date", "ticker_symbol").orderBy(
        F.col("_staged_at").desc()
    )

    df_int = (
        df_stg_assets.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .select(
            F.md5(
                F.concat_ws(
                    "||",
                    F.col("price_date").cast("string"),
                    F.col("ticker_symbol"),
                )
            ).alias("asset_key"),
            F.col("price_date"),
            F.col("ticker_symbol"),
            F.col("close_price"),
            F.current_timestamp().alias("_intermediate_at"),
        )
    )

    # Simpan ke Silver Parquet
    df_int.coalesce(1).write.mode("overwrite").parquet(assets_silver_path)


if __name__ == "__main__":
    # Boleh override melalui command line argumen: python staging.py <nama_bucket>
    bucket = sys.argv[1] if len(sys.argv) > 1 else "nama-bucket-default-anda"

    spark_session = get_spark_session()
    print(f"Memulakan transformasi Silver untuk bucket: {bucket}")

    process_silver_forex(spark_session, bucket)
    print("Selesai proses Silver Forex.")

    process_silver_assets(spark_session, bucket)
    print("Selesai proses Silver Assets.")

    spark_session.stop()
