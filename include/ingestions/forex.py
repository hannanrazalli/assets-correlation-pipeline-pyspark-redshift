import json
import logging
import os
from datetime import UTC, datetime

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def daily_forex(ds: str | None = None):
    if not ds:
        ds = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    url = "https://api.frankfurter.dev/v2/rates"
    params = {
        "base": "usd",
        "quotes": "myr",
        "date": ds,
    }

    response = requests.get(url, params)
    response.raise_for_status()
    data = response.json()

    ndjson = "\n".join([json.dumps(d) for d in data])
    dt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=UTC)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_bucket = os.getenv("BUCKET_NAME")
    s3_key = f"raw/forex/year={year}/month={month}/day={day}/forex_{dt.strftime('%Y%m%d')}.ndjson"

    s3_hook.load_string(
        string_data=ndjson,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True,
    )

    logger.info(f"[SUCCESS] Successfully ibgest data for date: {ds}")

    return ndjson
