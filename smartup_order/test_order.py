import urllib
from datetime import datetime, timedelta

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
from sqlalchemy.types import Float, Integer, String, DateTime, Boolean


# 🔹 Cookies olish
def get_cookies_from_browser(url):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Login qilib bo‘lgach Enter ni bosing...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


# 🔹 DataFrame typelarni avtomatik o‘zgartirish
def auto_cast_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        try:
            s = df[col].dropna().astype(str)

            if s.empty:
                continue

            # Boolean
            if s.str.lower().isin(['true', 'false']).all():
                df[col] = s.str.lower().map({'true': 1, 'false': 0}).astype('Int64')
                continue

            # Integer
            if s.str.fullmatch(r"\d+").all():
                df[col] = pd.to_numeric(s, downcast='integer', errors='coerce')
                continue

            # Float
            if s.str.fullmatch(r"\d+\.\d+").all():
                df[col] = pd.to_numeric(s, errors='coerce')
                continue

            # Date (dd.mm.yyyy)
            dt = pd.to_datetime(df[col], format="%d.%m.%Y", errors="coerce")
            if dt.notna().any():
                df[col] = dt
                continue

            # Date (yyyy-mm-dd)
            dt = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")
            if dt.notna().any():
                df[col] = dt

        except Exception:
            continue
    return df


# 🔹 JSON flatten qilish
def fetch_and_flatten(data_url, cookies, date_from, date_to):
    try:
        print(f"⬇️ Yuklanmoqda: {date_from} → {date_to}")
        response = requests.post(
            data_url,
            cookies=cookies,
            json={
                "begin_deal_date": datetime.strptime(date_from, "%Y-%m-%d").strftime("%d.%m.%Y"),
                "end_deal_date": datetime.strptime(date_to, "%Y-%m-%d").strftime("%d.%m.%Y")
            }
        )
        response.raise_for_status()
        data = response.json()

        orders = data.get("order", [])
        if not orders:
            print("⚠️ Bu davrda 'order' topilmadi")
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
            order_id = product.get("order_id")
            product_id = product.get("product_id")
            for detail in product.get("details", []):
                detail["order_id"] = order_id
                detail["product_id"] = product_id
                details_list.append(detail)
        details_df = pd.DataFrame(details_list)

        # Dublikatlarni olib tashlash
        if "deal_id" in order_df.columns:
            order_df = order_df.drop_duplicates(subset=["deal_id"])
        if {"order_id", "product_id"}.issubset(order_products_df.columns):
            order_products_df = order_products_df.drop_duplicates(subset=["order_id", "product_id"])
        if {"order_id", "product_id"}.issubset(details_df.columns):
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


# 🔹 Limit bo‘yicha bo‘lish
def safe_fetch(data_url, cookies, date_from, date_to, limit=7900):
    result = []
    stack = [(date_from, date_to)]

    while stack:
        start, end = stack.pop()
        data = fetch_and_flatten(data_url, cookies, start, end)
        if not data:
            continue

        count = len(data["order_main"])
        if count >= limit:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            mid_dt = start_dt + (end_dt - start_dt) / 2
            stack.append((mid_dt.strftime("%Y-%m-%d"), end))
            stack.append((start, mid_dt.strftime("%Y-%m-%d")))
        else:
            result.append(data)
    return result


# 🔹 SQL Serverga yozish (dublikatlarsiz)
def upload_to_sql(df_dict):
    try:
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=Epco;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        for table_name, df in df_dict.items():
            if df is None or df.empty or df.columns.empty:
                print(f"⏭ {table_name} bo‘sh – o‘tkazib yuborildi.")
                continue

            # Auto cast
            df = auto_cast_dataframe(df)

            # Unique key bo‘yicha dublikatlarni tashlash
            if table_name == "order_main" and "deal_id" in df.columns:
                df = df.drop_duplicates(subset=["deal_id"])
            elif table_name == "order_products" and {"order_id", "product_id"}.issubset(df.columns):
                df = df.drop_duplicates(subset=["order_id", "product_id"])
            elif table_name == "order_details" and {"order_id", "product_id"}.issubset(df.columns):
                df = df.drop_duplicates(subset=["order_id", "product_id"])

            print(f"📥 {table_name} ({len(df)} ta satr) yozilmoqda...")

            # DataType mapping
            dtype_mapping = {}
            for col in df.columns:
                sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if sample_value is None:
                    dtype_mapping[col] = String(255)
                elif isinstance(sample_value, float):
                    dtype_mapping[col] = Float()
                elif isinstance(sample_value, int):
                    dtype_mapping[col] = Integer()
                elif isinstance(sample_value, bool):
                    dtype_mapping[col] = Boolean()
                elif hasattr(sample_value, "year"):  # datetime
                    dtype_mapping[col] = DateTime()
                else:
                    dtype_mapping[col] = String(500)

            # SQL ga append qilish
            df.to_sql(
                table_name,
                con=engine,
                index=False,
                if_exists="append",
                dtype=dtype_mapping
            )

        print("✅ SQL Serverga yozildi.")
    except Exception as e:
        print(f"❌ SQL yozishda xatolik: {e}")


# 🔹 Oyma-oy interval generator
def month_ranges(start_date, end_date):
    current = start_date
    while current < end_date:
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield current.strftime("%Y-%m-%d"), min(next_month, end_date).strftime("%Y-%m-%d")
        current = next_month


# 🔹 Main
if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"
    cookies = get_cookies_from_browser("https://smartup.online")

    # 🔹 Start doim 2025-01-01
    start_date = datetime(2025, 1, 1)

    # 🔹 End esa har kuni hozirgi sana
    end_date = datetime.today()

    for date_from, date_to in month_ranges(start_date, end_date):
        results = safe_fetch(DATA_URL, cookies, date_from, date_to)
        for df_dict in results:
            if df_dict:
                upload_to_sql(df_dict)
