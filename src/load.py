import pandas as pd, json
from db_config import get_conn
from pathlib import Path
BASE = Path(__file__).resolve().parent.parent

df = pd.read_csv(BASE / "data" / "clean.csv")
df["datetime_utc"] = pd.to_datetime(df["datetime_utc"])

conn = get_conn()
cur = conn.cursor()
sql = """
INSERT INTO gempa_idn(
datetime_utc, tanggal, jam, coordinates, magnitude, kedalaman, wilayah, potensi
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (datetime_utc) DO NOTHING
RETURNING datetime_utc;
"""
baru_masuk = []
for _, row in df.iterrows():
    cur.execute(sql, (
        row["datetime_utc"], row["tanggal"], row["jam"], row["coordinates"],
        row["magnitude"], row["kedalaman"], row["wilayah"], row["potensi"]
    ))
    hasil = cur.fetchone()
    if hasil:
        baru_masuk.append({
            "datetime_utc": str(row["datetime_utc"]),
            "magnitude": float(row["magnitude"]),
            "kedalaman": row["kedalaman"],
            "wilayah": row["wilayah"],
            "potensi": row["potensi"],
        })
conn.commit()
cur.close()
conn.close()

with open(BASE / "data" / "notif.json", "w") as f:
    json.dump(baru_masuk, f)
print(f"{len(baru_masuk)} data baru masuk database")
