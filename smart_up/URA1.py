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

def auto_flatten_json(data):
    if isinstance(data, dict) and len(data) == 1:
        content = list(data.values())[0]
    else:
        content = data

    if isinstance(content, list):
        return pd.json_normalize(content, sep="_", max_level=2)
    elif isinstance(content, dict):
        return pd.json_normalize([content], sep="_", max_level=2)
    else:
        raise ValueError("❌ Noto‘g‘ri JSON format.")

def fetch_and_flatten(data_url):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Ma'lumot yuklanmoqda...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()
        data = response.json()

        df = auto_flatten_json(data)
        print(f"✅ JSON'dan DataFrame yaratildi. {len(df)} ta satr.")
        return df
    except Exception as e:
        print(f"❌ Xatolik: {e}")
        return None

def upload_to_sql(df, table_name):
    try:
        print(f"🔌 SQL Serverga ulanmoqda, jadval: {table_name}")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=WIN-LORQJU2719N;"
            "DATABASE=Test;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        try:
            existing_df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
            df = pd.concat([existing_df, df]).drop_duplicates()
        except Exception:
            print("🆕 Jadval yangi, birinchi marta yozilmoqda.")

        df.to_sql(table_name, con=engine, index=False, if_exists="replace")
        print(f"✅ '{table_name}' jadvalga yozildi. {len(df)} ta satr.")
    except Exception as e:
        print(f"❌ SQL yozishda xatolik: {e}")

if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/anor/mxsx/mr/natural_person$export"
    TABLE_NAME = "natural_person"

    df = fetch_and_flatten(DATA_URL)
    if df is not None:
        upload_to_sql(df, table_name=TABLE_NAME)
