import json
import pandas as pd
import requests
from pandas import json_normalize
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
import urllib


def get_cookies_from_browser(url):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Login qilib bo‘lgach Enter ni bosing...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


def flatten_json_data(data):
    if isinstance(data, list):
        return json_normalize(data, sep="_", max_level=2)
    elif isinstance(data, dict):
        return json_normalize([data], sep="_", max_level=2)
    else:
        raise ValueError("Noto‘g‘ri JSON format!")


def fetch_and_process_data(data_url, csv_file):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Ma'lumot yuklanmoqda...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()
        data = response.json()

        df = flatten_json_data(data)
        df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        print(f"✅ CSV fayl saqlandi: {csv_file}")
        return df

    except Exception as e:
        print(f"❌ Xatolik: {e}")
        with open("smartup_export_error.txt", "wb") as f:
            f.write(response.content)
        return None


def upload_to_sql(df, table_name="smartup_data"):
    try:
        print("🔌 SQL Serverga ulanmoqda...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=Test;"
            "TrustServerCertificate=yes;"
            "Trusted_Connection=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        # Eski yozuvlarni olish va dublikate olib tashlash
        try:
            existing_df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
            print(f"📊 Eski yozuvlar: {len(existing_df)}")
            combined_df = pd.concat([existing_df, df])
            df_clean = combined_df.drop_duplicates()
        except Exception:
            print("🆕 Yangi jadval, to‘g‘ridan-to‘g‘ri yoziladi.")
            df_clean = df

        df_clean.to_sql(table_name, con=engine, index=False, if_exists="replace")
        print(f"✅ '{table_name}' jadvalga {len(df_clean)} ta noyob yozuv yozildi.")
    except Exception as e:
        print(f"❌ SQL yozishda xatolik: {e}")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"
    CSV_FILE = "smartup_order_export.csv"
    df = fetch_and_process_data(DATA_URL, CSV_FILE)
    if df is not None:
        upload_to_sql(df)
