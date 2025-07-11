import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
import urllib


def get_cookies_from_browser(url):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Saytga login bo‘lgach Enter ni bosing...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


def auto_cast_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        s = df[col].dropna().astype(str)

        # Bool → Int64
        if s.str.lower().isin(['true', 'false']).all():
            df[col] = s.str.lower().map({'true': 1, 'false': 0}).astype('Int64')
            continue

        # Integer
        if s.str.fullmatch(r"\d+").all():
            df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
            continue

        # Float
        if s.str.fullmatch(r"\d+\.\d+").all():
            df[col] = pd.to_numeric(df[col], errors='coerce')
            continue

        # Datetime
        try:
            df[col] = pd.to_datetime(df[col], errors='raise', dayfirst=True)
        except Exception:
            continue

    return df


def fetch_inventory_tables(inventory_url, group_url):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Ma'lumotlar yuklanmoqda...")

        inv_data = requests.get(inventory_url, cookies=cookies).json()
        group_data = requests.get(group_url, cookies=cookies).json()

        inventory_raw = inv_data.get("inventory", [])

        # ---------------- inventory ----------------
        inv_df = pd.json_normalize(inventory_raw)

        # ---------------- inventory_groups ----------------
        group_rows = []
        for item in inventory_raw:
            pid = item.get("product_id")
            code = item.get("code")
            for g in item.get("groups", []):
                group_rows.append({
                    "product_id": pid,
                    "code": code,
                    "group_id": g.get("group_id"),  # ✅ TUZATILDI
                    "group_code": g.get("group_code"),
                    "type_id": g.get("type_id")
                })
        group_df = pd.DataFrame(group_rows)

        # ---------------- inventory_kinds ----------------
        kind_rows = []
        for item in inventory_raw:
            pid = item.get("product_id")
            code = item.get("code")
            for k in item.get("inventory_kinds", []):
                kind_rows.append({
                    "product_id": pid,
                    "code": code,
                    "kind_code": k.get("kind_code"),  # ✅ TUZATILDI
                    "state": k.get("state")
                })
        kind_df = pd.DataFrame(kind_rows)

        # ---------------- inventory_return ----------------
        return_rows = []
        for item in inventory_raw:
            pid = item.get("product_id")
            code = item.get("code")
            for r in item.get("return_conditions", []):
                return_rows.append({
                    "product_id": pid,
                    "code": code,
                    "condition_code": r.get("condition_code"),
                    "is_default": r.get("is_default")
                })
        return_df = pd.DataFrame(return_rows)

        # ---------------- inventory_sectors ----------------
        sector_rows = []
        for item in inventory_raw:
            pid = item.get("product_id")
            code = item.get("code")
            for s in item.get("sector_codes", []):
                sector_code = s.get("sector_code") if isinstance(s, dict) else s
                sector_rows.append({
                    "product_id": pid,
                    "code": code,
                    "sector_code": sector_code
                })
        sector_df = pd.DataFrame(sector_rows)

        # Auto-cast all tables
        df_dict = {
            "inventory": inv_df,
            "inventory_groups": group_df,
            "inventory_kinds": kind_df,
            "inventory_return": return_df,
            "inventory_sectors": sector_df
        }

        for name, df in df_dict.items():
            if not df.empty:
                df_dict[name] = auto_cast_dataframe(df)
                print(f"✅ {name}: {len(df)} satr, turlar avtomatik to‘g‘irlandi")

        return df_dict

    except Exception as e:
        print(f"❌ Xatolik: {e}")
        return None


def upload_to_sql(df_dict):
    try:
        print("🔌 SQL Serverga ulanmoqda...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=DealDB;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        for table_name, df in df_dict.items():
            if df.empty or df.columns.empty:
                print(f"⏭ {table_name} bo‘sh — o‘tkazib yuborildi.")
                continue
            print(f"📥 {table_name} ({len(df)} satr) yozilmoqda...")
            df.to_sql(table_name, con=engine, index=False, if_exists="replace")

        print("✅ Barcha jadvallar SQL Serverga muvaffaqiyatli yozildi.")

    except Exception as e:
        print(f"❌ SQL yozishda xatolik: {e}")


if __name__ == "__main__":
    DATA_URL_INVENTORY = "https://smartup.online/b/anor/mxsx/mr/inventory$export"
    DATA_URL_GROUPS = "https://smartup.online/b/anor/mxsx/mr/product_group$export"

    df_dict = fetch_inventory_tables(DATA_URL_INVENTORY, DATA_URL_GROUPS)
    if df_dict:
        upload_to_sql(df_dict)
