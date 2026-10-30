import json

orders_raw = [
    {
        "order_id": "ORD_101",
        "order_date": "2026-10-30",
        "store": "KLCC Branch",
        "items": [
            {"sku": "SKU_A", "qty": 2, "price": 10.0},
            {"sku": "SKU_B", "qty": 1, "price": 25.0},
        ],
    },
    {
        "order_id": "ORD_102",
        "order_date": "2026-10-30",
        "store": "MidValley Branch",
        "items": [{"sku": "SKU_C", "qty": 5, "price": 4.5}],
    },
]

records = []

for o in orders_raw:
    order_id = o.get("order_id")
    order_date = o.get("order_date")
    store = o.get("store")

    items = o.get("items") or []
    for i in items:
        rows = {
            "order_id": order_id,
            "order_date": order_date,
            "store": store,
            "sku": i.get("sku"),
            "qty": i.get("qty"),
            "price": i.get("price"),
        }
        records.append(json.dumps(rows))

ndjson = "\n".join(records)
print(ndjson)
