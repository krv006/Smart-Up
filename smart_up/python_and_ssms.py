import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from pandas import json_normalize
import sqlalchemy


# ✅ SSMS Ulanish sozlamalari
SERVER = '213.230.120.114:7002'
DATABASE = 'Test'
DRIVER = 'ODBC+Driver+17+for+SQL+Server'
TABLE_NAME = 'SmartupOrders'

# ✅ Foydali funksiya: cookie olish
def get_cookies_from_browser(url):
    print("🌐 Brauzer ochilmoqda... Login qiling...")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", False)
    chrome_options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    input("⏳ Tizimga kirib bo‘lgach Enter bosing...")

    cookies = driver.get_cookies()
    driver.quit()

    return {cookie['name']: cookie['value'] for cookie in cookies}


# ✅ Nested list kalitlarini topish
def explore_json(data, prefix=""):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                yield from explore_json(value, f"{prefix}{key}.")
    elif isinstance(data, list) and data:
        yield prefix[:-1]


# ✅ JSON flatten funksiyasi
def flatten_json_data(data):
    if isinstance(data, list):
        return json_normalize(data, sep="_", max_level=2)
    elif isinstance(data, dict):
        return json_normalize([data], sep="_", max_level=2)
    else:
        raise ValueError("Noto‘g‘ri JSON format!")


# ✅ Asosiy ish: JSON olish, CSV saqlash, DB ga yozish
def fetch_and_store_to_sql(data_url, json_file, csv_file):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")

        print("⬇️ Ma'lumot yuklanmoqda...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()

        print("🔍 JSON pars qilinmoqda...")
        data = response.json()

        # Nested list topamiz
        list_keys = list(explore_json(data))
        print(f"🔎 Topilgan nested listlar: {list_keys if list_keys else 'Yo‘q'}")

        df = None
        if list_keys:
            for key in list_keys:
                try:
                    nested_data = data
                    for part in key.split("."):
                        nested_data = nested_data[part]
                    df = json_normalize(nested_data, sep="_", max_level=2)
                    print(f"✅ '{key}' dan DataFrame yaratildi")
                    break
                except Exception:
                    continue
        else:
            df = flatten_json_data(data)

        # === Tozalash ===
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass

        # ✅ Fayllarni saqlash
        df.to_json(json_file, orient="records", indent=4, force_ascii=False)
        df.to_csv(csv_file, index=False)
        print(f"✅ JSON saqlandi: {json_file}")
        print(f"✅ CSV saqlandi: {csv_file}")

        # ✅ SQL Serverga yozish
        connection_str = (
            f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
            f"?driver={DRIVER}"
        )
        engine = sqlalchemy.create_engine(connection_str)
        df.to_sql(name=TABLE_NAME, con=engine, if_exists="replace", index=False)
        print(f"✅ SQL Serverdagi '{TABLE_NAME}' jadvalga muvaffaqiyatli yozildi!")

    except Exception as e:
        print(f"❌ Xatolik: {e}")
        with open("smartup_error.txt", "wb") as f:
            f.write(response.content)
        print("📜 Xom javob saqlandi: smartup_error.txt")
        try:
            print("📜 JSON namunasi (birinchi 500 belgi):")
            print(json.dumps(data, indent=2)[:500])
        except:
            print("📜 JSONni chiqarib bo‘lmadi")


# ✅ Boshlanish
if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"
    JSON_FILE = "smartup_order_export.json"
    CSV_FILE = "smartup_order_export.csv"

    fetch_and_store_to_sql(DATA_URL, JSON_FILE, CSV_FILE)
