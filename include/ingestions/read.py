import json
from datetime import UTC, datetime

import requests
from dotenv import load_dotenv

load_dotenv()


def daily_forex(date_str: str | None = None):
    if not date_str:
        date_str = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    url = "https://api.frankfurter.dev/v2/rates"
    params = {
        "date": date_str,
        "base": "USD",
        "quotes": "MYR",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    ndjson = "\n".join([json.dumps(r) for r in data])
    print(ndjson)


daily_forex()
