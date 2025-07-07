import json
import logging
import sys
import time
from typing import Optional

import pandas as pd
import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EMAIL = "powerbi@epco.com"
PASSWORD = "said_2021"
LOGIN_URL = "https://smartup.online"
DASHBOARD_URL = "https://smartup.online/#/!44lnbqonn/trade/intro/dashboard"
DATA_URL = "https://smartup.online/b/anor/mxsx/mdeal/return$export"
OUTPUT_FILE = "smartup_return_export.json"

def setup_chrome_driver() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', { get: () => undefined })"
    })
    logger.info("✅ Chrome driver tayyor")
    return driver


def get_cookies_with_login(login_url, dashboard_url, email, password) -> dict:
    driver = setup_chrome_driver()
    try:
        logger.info("🌐 Saytga kirilmoqda...")
        driver.get(login_url)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(5)

        driver.save_screenshot("login_page.png")
        with open("login_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        logger.info("⌨️ Login ma'lumotlari kiritilmoqda...")
        email_input = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//input[contains(@type, 'email') or contains(@placeholder, 'pochta')]"))
        )
        password_input = driver.find_element(By.XPATH, "//input[@type='password']")
        login_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Kirish') or contains(text(), 'Login')]")

        email_input.clear()
        email_input.send_keys(email)
        password_input.clear()
        password_input.send_keys(password)
        login_btn.click()
        logger.info("🚀 Login bosildi...")

        WebDriverWait(driver, 30).until(EC.url_contains("dashboard"))
        logger.info("✅ Dashboard sahifasi yuklandi.")

        driver.get(dashboard_url)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")
        with open("dashboard_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        cookies = {c['name']: c['value'] for c in driver.get_cookies()}
        logger.info(f"🍪 {len(cookies)} ta cookie olindi.")
        return cookies

    except (TimeoutException, NoSuchElementException) as e:
        driver.save_screenshot("login_error.png")
        logger.error(f"❌ Login xatosi: {e}")
        raise RuntimeError("Login muvaffaqiyatsiz bo‘ldi")
    finally:
        driver.quit()
        logger.info("🧹 Browser yopildi.")


def explore_json(data, prefix="") -> list:
    keys = []
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                keys.extend(explore_json(v, f"{prefix}{k}."))
    elif isinstance(data, list) and data:
        keys.append(prefix[:-1])
    return keys


def fetch_and_export_data(data_url, output_file, cookies):
    logger.info("⬇️ Ma'lumotlar yuklanmoqda...")
    try:
        r = requests.get(data_url, cookies=cookies, timeout=30)
        r.raise_for_status()
        data = r.json()

        list_keys = explore_json(data)
        df = None

        if list_keys:
            for key in list_keys:
                try:
                    sub = data
                    for part in key.split("."):
                        sub = sub[part]
                    df = pd.json_normalize(sub)
                    break
                except Exception:
                    continue
        else:
            df = pd.DataFrame(data if isinstance(data, list) else [data])

        if df is not None and not df.empty:
            df.to_json(output_file, orient="records", indent=2, force_ascii=False)
            logger.info(f"✅ Fayl saqlandi: {output_file}")
        else:
            raise ValueError("DataFrame bo‘sh")

    except Exception as e:
        logger.error(f"❌ Yuklashda xatolik: {e}")
        raise


if __name__ == "__main__":
    try:
        cookies = get_cookies_with_login(LOGIN_URL, DASHBOARD_URL, EMAIL, PASSWORD)
        fetch_and_export_data(DATA_URL, OUTPUT_FILE, cookies)
    except Exception as e:
        logger.error(f"🏁 Umumiy xatolik: {e}")
        sys.exit(1)
