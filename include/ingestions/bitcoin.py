import logging
import os
from datetime import UTC, datetime, timedelta

import awswrangler as wr
import yfinance as yf
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def daily_bitcoin(ds: str | None = None):
    if ds is None:
        ds = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    dt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=UTC)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    start_date = dt.strftime("%Y-%m-%d")
    end_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    df = yf.download(tickers=["BTC-USD"], start=start_date, end=end_date, interval="1d")

    if df is None:
        print(f"[EMPTY] Market closed on {ds}. Skipping...")
        return

    df_close = df["Close"].reset_index().rename(columns={"BTC-USD": "BTC_USD"})

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_session = s3_hook.get_session()
    s3_bucket = os.getenv("BUCKET_NAME")
    s3_key = f"s3://{s3_bucket}/raw/bitcoin/year={year}/month={month}/day={day}/bitcoin_{dt.strftime('%Y%m%d')}.parquet"

    wr.s3.to_parquet(
        df=df_close,
        path=s3_key,
        boto3_session=s3_session,
        dataset=False,
    )

    logger.info(df_close.tail())
    logger.info(f"[SUCCESS] Successfully ingest Bitcoin API date: {ds}")
