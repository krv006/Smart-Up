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
    input("🌐 Saytga login bo‘ling va Enter bosing...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


def auto_cast_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        s = df[col].dropna().astype(str)

        # Boolean
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


def fetch_and_flatten(data_url):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Ma'lumotlar yuklanmoqda...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    data = value
                    break
            else:
                raise ValueError("❌ JSON strukturasida ro‘yxat topilmadi.")
        elif not isinstance(data, list):
            raise ValueError("❌ JSON noto‘g‘ri formatda.")

        # --- Asosiy jadval ---
        groups_df = pd.json_normalize(data, sep="_", max_level=1)

        # --- Ichki product_group_types jadvali ---
        group_types_list = []
        for group in data:
            group_id = group.get("product_group_id")
            for gtype in group.get("product_group_types", []):
                gtype["product_group_id"] = group_id
                group_types_list.append(gtype)
        group_types_df = pd.DataFrame(group_types_list)

        df_dict = {
            "product_group": groups_df,
            "product_group_types": group_types_df
        }

        # Tiplarni avtomatik aniqlash
        for name, df in df_dict.items():
            if not df.empty and not df.columns.empty:
                df_dict[name] = auto_cast_dataframe(df)
                print(f"✅ {name}: {len(df)} satr, tiplar konvertatsiya qilindi.")

        return df_dict

    except Exception as e:
        print(f"❌ Xatolik: {e}")
        return None


def upload_to_sql(df_dict):
    try:
        print("🔌 SQL Server’ga ulanmoqda...")
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

        print("✅ Barcha jadval SQL Server’ga muvaffaqiyatli yozildi.")

    except Exception as e:
        print(f"❌ SQL yozishda xatolik: {e}")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/anor/mxsx/mr/product_group$export"
    df_dict = fetch_and_flatten(DATA_URL)
    if df_dict:
        upload_to_sql(df_dict)
