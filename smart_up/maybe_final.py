import json

import pandas as pd
import requests
from pandas import json_normalize
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_cookies_from_browser(url):
    print("🌐 Brauzer ochilmoqda... Login qiling...")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", False)
    chrome_options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    input("⏳ Iltimos, tizimga kirib bo‘lgach Enter ni bosing...")

    cookies = driver.get_cookies()
    driver.quit()

    return {cookie['name']: cookie['value'] for cookie in cookies}


def explore_json(data, prefix=""):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                yield from explore_json(value, f"{prefix}{key}.")
    elif isinstance(data, list) and data:
        yield prefix[:-1]


def flatten_json_data(data):
    if isinstance(data, list):
        return json_normalize(data, sep="_", max_level=2)
    elif isinstance(data, dict):
        return json_normalize([data], sep="_", max_level=2)
    else:
        raise ValueError("Noto‘g‘ri JSON format!")


def fetch_and_export_data(data_url, output_json_file, output_csv_file):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")

        print("⬇️ Ma'lumot yuklanmoqda...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '').lower()
        print(f"📄 Content-Type: {content_type}")

        print("🔍 JSON pars qilinmoqda...")
        data = response.json()

        list_keys = list(explore_json(data))
        print(f"🔎 Topilgan ro'yxat kalitlari: {list_keys if list_keys else 'Hech qanday ro‘yxat topilmadi'}")

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
                except (KeyError, TypeError):
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

        # === Saqlash ===
        df.to_json(output_json_file, orient="records", indent=4, force_ascii=False)
        df.to_csv(output_csv_file, index=False)
        print(f"✅ JSON fayl saqlandi: {output_json_file}")
        print(f"✅ CSV fayl saqlandi: {output_csv_file}")
        print("📊 DataFrame namunasi (birinchi 2 qator):")
        print(df.head(2).to_string())

    except Exception as e:
        print(f"❌ Xatolik: {e}")
        with open("smartup_export_error.txt", "wb") as f:
            f.write(response.content)
        print("📜 Xom javob smartup_export_error.txt ga saqlandi.")
        try:
            print("📜 JSON namunasi (birinchi 500 belgi):")
            print(json.dumps(data, indent=2)[:500])
        except:
            print("📜 JSONni chiqarib bo‘lmadi")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"
    JSON_FILE = "smartup_order_export.json"
    CSV_FILE = "smartup_order_export_test.csv"

    fetch_and_export_data(DATA_URL, JSON_FILE, CSV_FILE)
