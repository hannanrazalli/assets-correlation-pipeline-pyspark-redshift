import logging
import os
import time
from datetime import UTC, datetime, timedelta

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

import awswrangler as wr
import yfinance as yf

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def daily_gold(ds: str | None = None):
    if ds is None:
        ds = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    dt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=UTC)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    start_date = dt.strftime("%Y-%m-%d")
    end_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    df = yf.download(tickers=["GLD"], start=start_date, end=end_date, interval="1d")

    if df is None:
        print(f"[EMPTY] Market closed on {ds}. Skipping...")
        return

    df_close = df["Close"].reset_index()

    s3_session = boto3.Session()
    s3_bucket = os.getenv("BUCKET_NAME")
    s3_key = f"s3://{s3_bucket}/raw/gold/year={year}/month={month}/day={day}/gold_{dt.strftime('%Y%m%d')}.parquet"

    wr.s3.to_parquet(
        df=df_close,
        path=s3_key,
        boto3_session=s3_session,
        dataset=False,
    )

    logger.info(df_close.tail())
    logger.info(f"[SUCCESS] Successfully ingest Gold API date: {ds}")


def backfill(start, end):
    dates = pd.date_range(start=start, end=end).strftime("%Y-%m-%d")

    for date in dates:
        daily_gold(date)

        time.sleep(1)

    logger.info(f"[COMPLETED] Finish ingesting for {len(dates)} dates")


start = "2026-11-17"
end = "2026-11-21"
backfill(start, end)
