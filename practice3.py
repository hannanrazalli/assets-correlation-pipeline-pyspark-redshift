import json

forex_payload = {
    "base": "USD",
    "date": "2026-10-30",
    "rates": {
        "MYR": 4.0657,
        "SGD": 1.3210,
        "EUR": 0.9245,
        "GBP": 0.7712,
    },
}

base = forex_payload.get("base")
date = forex_payload.get("date")
rates = forex_payload.get("rates")

records = []

for target, rate in rates.items():
    rows = {"date": date, "base": base, "target": target, "rate": rate}
    records.append(json.dumps(rows))

ndjson = "\n".join(records)
print(ndjson)
