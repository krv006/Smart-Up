import json
import pandas as pd
from pandas import json_normalize

with open("smartup_order_export.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)


def flatten_json_data(data):
    if isinstance(data, list):
        df = json_normalize(data, sep='_', max_level=2)
    elif isinstance(data, dict):
        df = json_normalize([data], sep='_', max_level=2)
    else:
        raise ValueError("Noto‘g‘ri JSON format!")
    return df

# === 3. Flatten qilamiz ===
df = flatten_json_data(raw_data)

# === 4. Har xil datalarni tozalaymiz ===
for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

# === 5. Raqamli ustunlarni aniqlab float/intga aylantirish (optional) ===
for col in df.columns:
    try:
        df[col] = pd.to_numeric(df[col], errors='ignore')
    except:
        pass

# === 6. CSV saqlash ===
df.to_csv("smartup_order_export.csv", index=False)
print("✅ Har qanday JSON fayl uchun flatten qilingan CSV tayyor.")
