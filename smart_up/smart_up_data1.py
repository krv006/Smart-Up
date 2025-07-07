import requests
import pandas as pd
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === STEP 1: Cookie'ni avtomatik olish uchun login qilamiz ===
def get_cookies_after_login(url, email, password):
    print("🔐 Avtomatik login boshlandi...")

    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Brauzer ko‘rinmasin
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    try:
        # ⌛ Login formani kutamiz
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "email")))

        driver.find_element(By.NAME, "email").send_keys(email)
        driver.find_element(By.NAME, "password").send_keys(password)

        # Submit button topamiz
        submit_btn = driver.find_element(By.XPATH, "//button[@type='submit']")
        submit_btn.click()

        # ⌛ Login tugaguncha kutamiz (masalan: sahifa o‘zgarsa yoki dashboard keladi)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(text(),'Dashboard') or contains(text(),'Boshqaruv')]"))
        )

        cookies = driver.get_cookies()
        print("✅ Cookie’lar olindi.")
        return {cookie['name']: cookie['value'] for cookie in cookies}

    except Exception as e:
        print(f"❌ Login muammosi: {e}")
        return {}

    finally:
        driver.quit()


# === STEP 2: JSONni tahlil qilish ===
def explore_json(data, prefix=""):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                yield from explore_json(value, f"{prefix}{key}.")
    elif isinstance(data, list) and data:
        yield prefix[:-1]


# === STEP 3: Asosiy ishchi funksiya ===
def fetch_and_export_data(data_url, export_file, email, password):
    try:
        cookies = get_cookies_after_login("https://smartup.online", email, password)
        if not cookies:
            raise Exception("Cookie olishda muammo bo‘ldi (login muvaffaqiyatsiz)")

        print("⬇️ Ma'lumot yuklanmoqda...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()

        data = response.json()
        list_keys = list(explore_json(data))
        print(f"🔎 Ro'yxat kalitlari: {list_keys if list_keys else 'Topilmadi'}")

        df = None
        if list_keys:
            for key in list_keys:
                try:
                    nested = data
                    for part in key.split("."):
                        nested = nested[part]
                    df = pd.json_normalize(nested, errors="ignore")
                    print(f"✅ DataFrame yaratildi: '{key}'")
                    break
                except Exception:
                    continue
        else:
            df = pd.DataFrame(data if isinstance(data, list) else [data])

        if df is not None and not df.empty:
            df.to_json(export_file, orient="records", indent=4, force_ascii=False)
            print(f"✅ Fayl saqlandi: {export_file}")
            print("📊 Data namuna:")
            print(df.head(2).to_string())
        else:
            print("❌ Bo‘sh yoki noto‘g‘ri formatdagi DataFrame.")

    except Exception as e:
        print(f"❌ Xatolik: {e}")
        try:
            with open("smartup_export.txt", "wb") as f:
                f.write(response.content)
        except:
            pass
        try:
            print(json.dumps(data, indent=2)[:500])
        except:
            pass


# === STEP 4: Ishga tushirish ===
if __name__ == "__main__":
    EMAIL = "powerbi@epco.com"
    PASSWORD = "said_2021"
    DATA_URL = "https://smartup.online/b/anor/mxsx/mdeal/return$export"
    OUTPUT_FILE = "smartup_return_export.json"
    fetch_and_export_data(DATA_URL, OUTPUT_FILE, EMAIL, PASSWORD)
