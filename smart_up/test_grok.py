from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os
import requests
import time

# 📋 Login ma'lumotlari
EMAIL = "POWERBI@epco.com"  # Email katta-kichik harflarga moslashtirilgan
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
    email_input.send_keys(EMAIL)
    password_input = driver.find_element(By.ID, "password")
    password_input.send_keys(PASSWORD)

    # 📥 Login tugmasini topish va bosish
    print("Login tugmasi bosilmoqda...")
    login_button = driver.find_elements(By.TAG_NAME, "button")
    for button in login_button:
        if "login" in button.text.lower() or "kirish" in button.text.lower():
            button.click()
            break
    else:
        print("❌ Login tugmasi topilmadi.")
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
        print(driver.page_source[:1000])  # Sahifa manbasining bir qismini chop etish
    except:
        print("Sahifa manbasini olishda xatolik.")

finally:
    # 🔌 Brauzerni yopish
    try:
        driver.quit()
    except:
        pass
    print("Brauzer yopildi.")

# 📥 CSV faylni yuklash
if cookie_dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/csv, text/plain, */*",
    }

    try:
        print("CSV fayl yuklanmoqda...")
        response = requests.get(EXPORT_URL, headers=headers, cookies=cookie_dict)

        if response.status_code == 200:
            with open("smartup_export.csv", "wb") as f:
                f.write(response.content)
            print("✅ CSV fayl muvaffaqiyatli saqlandi: smartup_export.csv")
        else:
            print(f"❌ CSV yuklab olishda xatolik: Status kodi {response.status_code}")
            print(f"Xabar: {response.text[:500]}")

    except Exception as e:
        print("❌ So'rov yuborishda xatolik:", str(e))
else:
    print("❌ Cookie’lar olinmadi, CSV faylni yuklab bo‘lmadi.")
