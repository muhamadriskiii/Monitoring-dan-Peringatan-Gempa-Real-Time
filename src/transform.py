import pandas as pd
from pathlib import Path
BASE = Path(__file__).resolve().parent.parent

df = pd.read_json(BASE / "data" / "raw.json")
print(df)
print(df.info())

df.rename(columns={"DateTime": "datetime_utc"}, inplace=True)
df.columns = df.columns.str.lower()
for all in df.select_dtypes(include="str").columns:
    df[all] = df[all].str.lower()
df = df.drop_duplicates(subset=["datetime_utc"])
df.to_csv(BASE / "data" / "clean.csv", index=False)
