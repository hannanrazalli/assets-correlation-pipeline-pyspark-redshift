import os
from datetime import datetime

import awswrangler as wr
import boto3
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

start_date = "2021-01-01"
end_date = "2026-11-19"
df = yf.download(tickers=["BTC-USD"], start=start_date, end=end_date)

df_close = df["Close"].reset_index().rename(columns={"BTC-USD": "BTC_USD"})


df_close["year"] = df_close["Date"].dt.strftime("%Y")
df_close["month"] = df_close["Date"].dt.strftime("%m")
df_close["day"] = df_close["Date"].dt.strftime("%d")

dt = datetime.now().strftime("%Y-%m-%d")

s3_session = boto3.Session()
s3_bucket = os.getenv("BUCKET_NAME")
s3_key = f"s3://{s3_bucket}/raw/bitcoin/"

wr.s3.to_parquet(
    df=df_close,
    path=s3_key,
    boto3_session=s3_session,
    dataset=True,
    partition_cols=["year", "month", "day"],
    mode="overwrite_partitions",
)

print(df_close.tail())
print("[SUCCESS] Successfuklly ingest bitcoin api")
