import json

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_cookies_from_browser(url):
    print("🌐 Brauzer ochilmoqda... Login qiling...")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", False)  # optional: avtomatik yopish
    chrome_options.add_argument("--start-maximized")  # fullscreen

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
        yield prefix[:-1]  # Remove trailing dot


def fetch_and_export_data(data_url, output_file):
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
                    df = pd.json_normalize(nested_data, errors="ignore")
                    print(f"✅ '{key}' dan DataFrame yaratildi")
                    break
                except (KeyError, TypeError):
                    continue
        else:
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError("JSON ma'lumotlari noto‘g‘ri formatda")

        if df is not None and not df.empty:
            df.to_json(output_file, orient="records", indent=4, force_ascii=False)
            print(f"✅ JSON fayl saqlandi: {output_file}")
            print("📊 DataFrame namunasi (birinchi 2 qator):")
            print(df.head(2).to_string())
        else:
            raise ValueError("DataFrame bo‘sh yoki noto‘g‘ri")

    except Exception as e:
        print(f"❌ Xatolik: {e}")
        with open("smartup_export.txt", "wb") as f:
            f.write(response.content)
        print("📜 Xom javob smartup_export.txt ga saqlandi.")
        try:
            print("📜 JSON namunasi (birinchi 500 belgi):")
            print(json.dumps(data, indent=2)[:500])
        except:
            print("📜 JSONni chiqarib bo‘lmadi")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/anor/mxsx/mdeal/return$export"
    OUTPUT_FILE = "smartup_return_export.json"
    fetch_and_export_data(DATA_URL, OUTPUT_FILE)
