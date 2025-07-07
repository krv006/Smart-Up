import pandas as pd
from sqlalchemy import create_engine
import urllib

print("📥 CSV fayl o'qilmoqda...")
df = pd.read_csv("smartup_order_export.csv")
print(f"✅ {len(df)} ta satr yuklandi.")

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=WIN-LORQJU2719N;" # todo localhost bn xal qilsa boladi serverda  
    "DATABASE=Test;"
    "TrustServerCertificate=yes;"
    "Trusted_Connection=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

table_name = "smartup_data"

try:
    print("⬆️ Ma’lumotlar bazaga yozilmoqda...")
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")
    print(f"✅ {table_name} jadvalga muvaffaqiyatli yozildi.")
except Exception as e:
    print(f"❌ Xatolik: {e}")
