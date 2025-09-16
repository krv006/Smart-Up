
import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine, Integer, Float, String, DateTime, Boolean
import urllib


def get_cookies_from_browser(url):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Зайдите на сайт и нажмите Enter после авторизации...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}



def fetch_and_flatten(data_url):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Загружаем данные...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()
        data = response.json()

        # --- JSON ichidan asosiy listni olish ---
        if "visit" not in data:
            raise ValueError("❌ JSON formatida 'visit' topilmadi")

        visits = data["visit"]

        # visit_headers flatten
        visit_headers = []
        stocks = []
        merchandisings = []
        quizzes = []
        comments = []

        for v in visits:
            for h in v.get("visit_headers", []):
                visit_headers.append(h)

            for s in v.get("stocks", []):
                s["visit_id"] = v["visit_headers"][0]["visit_id"] if v.get("visit_headers") else None
                stocks.append(s)

            for m in v.get("merchandisings", []):
                m["visit_id"] = v["visit_headers"][0]["visit_id"] if v.get("visit_headers") else None
                merchandisings.append(m)

            for q in v.get("quizzes", []):
                q["visit_id"] = v["visit_headers"][0]["visit_id"] if v.get("visit_headers") else None
                quizzes.append(q)

            for c in v.get("comments", []):
                c["visit_id"] = v["visit_headers"][0]["visit_id"] if v.get("visit_headers") else None
                comments.append(c)

        # DataFrame’lar
        df_dict = {}
        if visit_headers:
            df_dict["visit_headers"] = pd.DataFrame(visit_headers)
        if stocks:
            df_dict["visit_stocks"] = pd.DataFrame(stocks)
        if merchandisings:
            df_dict["visit_merchandisings"] = pd.DataFrame(merchandisings)
        if quizzes:
            df_dict["visit_quizzes"] = pd.DataFrame(quizzes)
        if comments:
            df_dict["visit_comments"] = pd.DataFrame(comments)

        print("✅ Flatten qilingan DF lar:", [k for k in df_dict.keys()])
        return df_dict

    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")
        return None


def map_sql_types(df: pd.DataFrame):
    """Pandas dtypes -> SQLAlchemy types"""
    type_map = {}
    for col in df.columns:
        series = df[col].dropna()

        if pd.api.types.is_integer_dtype(series):
            type_map[col] = Integer
        elif pd.api.types.is_float_dtype(series):
            type_map[col] = Float
        elif pd.api.types.is_bool_dtype(series):
            type_map[col] = Boolean
        elif pd.api.types.is_datetime64_any_dtype(series):
            type_map[col] = DateTime
        else:
            # String type, calculate max length
            try:
                max_len = int(series.astype(str).str.len().max())
            except:
                max_len = 255
            type_map[col] = String(length=min(max_len, 1000))  # uzun text uchun
    return type_map



def upload_to_sql(df_dict):
    try:
        print("🔌 Подключение к SQL Server...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=DealDB;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        for table_name, df in df_dict.items():
            print(f"📥 Загрузка в таблицу: {table_name} ({len(df)} строк)")
            column_types = map_sql_types(df)
            df.to_sql(table_name, con=engine, index=False, if_exists="replace", dtype=column_types)

        print("✅ Все данные успешно записаны в SQL Server с корректными типами.")

    except Exception as e:
        print(f"❌ Ошибка при записи в SQL: {e}")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tvt/visit$export"
    df_dict = fetch_and_flatten(DATA_URL)
    if df_dict:
        upload_to_sql(df_dict)
