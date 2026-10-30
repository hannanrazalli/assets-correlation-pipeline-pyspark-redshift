import json

schools = {
    "sekolah": "SMK Sri Sentosa",
    "tingkatan": "Form 5",
    "kelas": [
        {
            "nama_kelas": "5 Alpha",
            "guru_tingkatan": "Cikgu Hafiz",
            "pelajar": [
                {"id": "P01", "nama": "Amir", "gred": "A"},
                {"id": "P02", "nama": "Bella", "gred": "B"},
            ],
        },
        {
            "nama_kelas": "5 Beta",
            "guru_tingkatan": "Cikgu Salmah",
            "pelajar": [
                {"id": "P03", "nama": "Chitash", "gred": "A"},
                {"id": "P04", "nama": "Danish", "gred": "C"},
            ],
        },
    ],
}

records = []


sekolah = schools.get("sekolah")
tingkatan = schools.get("tingkatan")
kelas = schools.get("kelas") or []

for k in kelas:
    pelajar = k.get("pelajar") or []

    for p in pelajar:
        rows = {
            "sekolah": sekolah,
            "tingkatan": tingkatan,
            "nama_kelas": k.get("nama_kelas"),
            "guru_tingkatan": k.get("guru_tingkatan"),
            "id": p.get("id"),
            "nama": p.get("nama"),
            "gred": p.get("gred"),
        }
        records.append(json.dumps(rows))

ndjson = "\n".join(records)
print(ndjson)
