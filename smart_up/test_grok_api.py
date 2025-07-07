import os
import json
import pandas as pd
import requests
import io
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time

# 📋 Login ma'lumotlari
EMAIL = "POWERBI@epco.com"
PASSWORD = "said_2021"
URL = "https://smartup.online"
EXPORT_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"

# 📂 ChromeDriver yo'lini aniqlash
CHROMEDRIVER_PATH = os.path.join(os.path.dirname(__file__), "chromedriver.exe")

# 📌 ChromeDriver mavjudligini tekshirish
if not os.path.exists(CHROMEDRIVER_PATH):
    print(f"❌ Xatolik: ChromeDriver {CHROMEDRIVER_PATH} da topilmadi. Iltimos, ChromeDriver-ni https://chromedriver.chromium.org/downloads dan yuklab oling.")
    exit(1)

# 🌐 Chrome sozlamalari
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Ko'rinmaydigan rejim (test uchun o'chirib turamiz)
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--window-size=1920,1080")

# 🍪 Cookie-larni saqlash uchun o'zgaruvchi
cookie_dict = {}

try:
    # 🌐 Brauzerni ishga tushirish
    driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, options=chrome_options)
    driver.get(URL)

    # ⏳ Email input borligini kutish
    print("Login sahifasi yuklanmoqda...")
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "email"))
    )
    print("Email input topildi.")

    # 🧑‍💻 Login formni to‘ldirish
    email_input = driver.find_element(By.ID, "email")
    email_input.clear()  # Inputni tozalash
    email_input.send_keys(EMAIL)
    password_input = driver.find_element(By.ID, "password")
    password_input.clear()  # Inputni tozalash
    password_input.send_keys(PASSWORD)

    # 📥 Login tugmasini topish va bosish
    print("Login tugmasi bosilmoqda...")
    try:
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'login') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'kirish')]"))
        )
        login_button.click()
    except:
        print("❌ Login tugmasi topilmadi (XPath orqali). HTML tuzilishini tekshiring.")
        raise Exception("Login button not found.")

    # ✅ Login muvaffaqiyatli bo'lishini kutish
    print("Dashboard yuklanmoqda...")
    WebDriverWait(driver, 30).until(EC.url_contains("/dashboard"))
    print("✅ Login muvaffaqiyatli bo'ldi.")

    # 🍪 Cookie-larni olish
    time.sleep(2)  # Cookie-lar to'liq yuklanishi uchun kutish
    cookies = driver.get_cookies()
    cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
    print("✅ Cookie’lar muvaffaqiyatli olindi:", cookie_dict)

except Exception as e:
    print("❌ Xatolik yuz berdi:", str(e))
    print("HTML sahifa manbasi (diagnostika uchun):")
    try:
        print(driver.page_source[:1000])
    except:
        print("Sahifa manbasini olishda xatolik.")

finally:
    # 🔌 Brauzerni yopish
    try:
        driver.quit()
    except:
        pass
    print("Brauzer yopildi.")

# 📥 Ma'lumotlarni tortib olish va JSON'ga aylantirish
if cookie_dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/csv, text/plain, application/json, */*",
    }

    try:
        print("Ma'lumotlar yuklanmoqda...")
        response = requests.get(EXPORT_URL, headers=headers, cookies=cookie_dict)

        if response.status_code == 200:
            # CSV ma'lumotlarini pandas orqali o'qish
            df = pd.read_csv(io.StringIO(response.text), encoding='utf-8', errors='ignore')
            # JSON'ga aylantirish
            json_data = df.to_dict(orient="records")

            # JSON faylga saqlash
            with open("smartup_export.json", "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=4)
            print("✅ Ma'lumotlar muvaffaqiyatli JSON shaklida saqlandi: smartup_export.json")

            # JSON ma'lumotlarning birinchi 2 qatorini ko'rsatish
            print("JSON ma'lumotlar (birinchi 2 qator):")
            print(json.dumps(json_data[:2], ensure_ascii=False, indent=4))

        else:
            print(f"❌ So‘rov muvaffaqiyatsiz: Status kodi {response.status_code}")
            print("Xabar:", response.text[:500])

    except Exception as e:
        print("❌ Ma'lumotlarni qayta ishlashda xatolik:", str(e))
        print("Server javobi (birinchi 500 belgi):", response.text[:500] if 'response' in locals() else "Javob mavjud emas")
else:
    print("❌ Cookie’lar olinmadi, ma'lumotlarni yuklab bo‘lmadi.")