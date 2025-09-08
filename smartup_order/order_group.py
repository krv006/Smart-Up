import urllib
from datetime import datetime, timedelta

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine


def get_cookies_from_browser(url):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Login qilib bo‘lgach Enter ni bosing...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


def auto_cast_dataframe(df):
    for col in df.columns:
        s = df[col].dropna().astype(str)

        if s.str.lower().isin(['true', 'false']).all():
            df[col] = s.str.lower().map({'true': 1, 'false': 0}).astype('Int64')
            continue

        if s.str.fullmatch(r"\d+").all():
            df[col] = pd.to_numeric(s, downcast='integer', errors='coerce')
            continue

        if s.str.fullmatch(r"\d+\.\d+").all():
            df[col] = pd.to_numeric(s, errors='coerce')
            continue

        try:
            df[col] = pd.to_datetime(df[col], errors='raise', dayfirst=True)
        except:
            continue
    return df


def fetch_and_flatten(data_url, cookies, date_from, date_to):
    try:
        print(f"⬇️ Yuklanmoqda: {date_from} → {date_to}")
        response = requests.post(
            data_url,
            cookies=cookies,
            json={"date_from": date_from, "date_to": date_to}
        )
        response.raise_for_status()
        data = response.json()

        orders = data.get("order", [])
        if not orders:
            print("⚠️ Bu oyda 'order' topilmadi")
            return None

        # order_main
        order_df = pd.json_normalize(orders, sep="_", max_level=1)

        # order_products
        order_products_list = []
        for order in orders:
            order_id = order.get("deal_id")
            for product in order.get("order_products", []):
                product["order_id"] = order_id
                order_products_list.append(product)
        order_products_df = pd.DataFrame(order_products_list)

        # order_details
        details_list = []
        for product in order_products_list:
            product_id = product.get("product_id")
            order_id = product.get("order_id")
            for detail in product.get("details", []):
                detail["product_id"] = product_id
                detail["order_id"] = order_id
                details_list.append(detail)
        details_df = pd.DataFrame(details_list)

        # Dublikatlarni olib tashlash
        if "deal_id" in order_df.columns:
            order_df = order_df.drop_duplicates(subset=["deal_id"])
        if "order_id" in order_products_df.columns:
            order_products_df = order_products_df.drop_duplicates(subset=["order_id", "product_id"])
        if "order_id" in details_df.columns:
            details_df = details_df.drop_duplicates(subset=["order_id", "product_id"])

        print(f"✅ {len(order_df)} order, {len(order_products_df)} product, {len(details_df)} detail")

        return {
            "order_main": order_df,
            "order_products": order_products_df,
            "order_details": details_df
        }
    except Exception as e:
        print(f"❌ Xatolik: {e}")
        return None


def upload_to_sql(df_dict):
    try:
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=WIN-LORQJU2719N;"
            "DATABASE=SmartUp;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        for table_name, df in df_dict.items():
            if df.empty or df.columns.empty:
                print(f"⏭ {table_name} bo‘sh – o‘tkazib yuborildi.")
                continue

            df = auto_cast_dataframe(df)

            print(f"📥 {table_name} ({len(df)} ta satr) yozilmoqda...")
            df.to_sql(table_name, con=engine, index=False, if_exists="append")

        print("✅ SQL Serverga yozildi.")
    except Exception as e:
        print(f"❌ SQL yozishda xatolik: {e}")


def month_ranges(start_date, end_date):
    """oyma-oy interval generatori"""
    current = start_date
    while current < end_date:
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield current.strftime("%Y-%m-%d"), min(next_month, end_date).strftime("%Y-%m-%d")
        current = next_month


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"
    cookies = get_cookies_from_browser("https://smartup.online")
    start_date = datetime(2025, 1, 1)
    end_date = datetime.today()
    for date_from, date_to in month_ranges(start_date, end_date):
        df_dict = fetch_and_flatten(DATA_URL, cookies, date_from, date_to)
        if df_dict:
            upload_to_sql(df_dict)
