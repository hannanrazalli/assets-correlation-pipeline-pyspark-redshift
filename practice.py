import json

raw = [
    {
        "transaction_id": "TXN_001",
        "timestamp": "2026-10-30T10:15:00Z",
        "customer": {"id": "CUST_99", "name": "Ali", "country": "MY"},
        "payment": {"method": "credit_card", "amount": 150.50, "currency": "MYR"},
    },
    {
        "transaction_id": "TXN_002",
        "timestamp": "2026-10-30T10:18:00Z",
        "customer": {"id": "CUST_88", "name": "Sarah", "country": "SG"},
        "payment": {"method": "e-wallet", "amount": 80.00, "currency": "SGD"},
    },
]

records = []

for r in raw:
    transaction_id = r.get("transaction_id")
    timestamp = r.get("timestamp")

    customer = r.get("customer") or {}
    payment = r.get("payment") or {}

    rows = {
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "id": customer.get("id"),
        "name": customer.get("name"),
        "country": customer.get("country"),
        "method": payment.get("method"),
        "amount": payment.get("amount"),
        "currency": payment.get("currency"),
    }
    records.append(json.dumps(rows))

ndjson = "\n".join(records)
print(ndjson)
