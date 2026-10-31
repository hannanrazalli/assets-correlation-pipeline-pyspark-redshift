import logging
import os
from datetime import UTC, datetime

import awswrangler as wr
import yfinance as yf
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def daily_stocks(ds: str | None = None):
    if not ds:
        ds = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    df = yf.download(["^GSPC", "^KLSE"], start="1d")
    df_close = df["Close"].reset_index()

    dt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=UTC)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    s3_hook = S3Hook(aws_conn_id="aws_default")

    s3_session = s3_hook.get_session()
    s3_bucket = os.getenv("BUCKET_NAME")
    s3_key = f"s3://{s3_bucket}/raw/stocks_test/year={year}/month={month}/day={day}/stocks_{dt.strftime('%Y%m%d')}.parquet"

    wr.s3.to_parquet(df=df_close, path=s3_key, boto3_session=s3_session, dataset=False)

    logger.info(f"[SUCCESS] Successfully ingest stock for date: {ds}")
