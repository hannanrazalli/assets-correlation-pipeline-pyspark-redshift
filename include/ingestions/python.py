import json
from datetime import UTC, datetime

import requests


def forex_daily(ds: str | None = None):
    if not ds:
        ds = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    url = "https://api.frankfurter.dev/v2/rates"
    params = {
        "base": "usd",
        "quotes": "myr",
        "from": "2026-10-25",
    }

    response = requests.get(url, params)
    response.raise_for_status()
    data = response.json()

    keys = ("date", "base", "quote", "rate")
    ndjson = "\n".join(json.dumps({k: r[k] for k in keys}) for r in data)
    print(ndjson)


forex_daily()
