import urllib
from datetime import datetime

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String, DateTime, Boolean


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

        if "visit" not in data:
            raise ValueError("❌ JSON formatida 'visit' topilmadi")

        visits = data["visit"]

        visit_headers, stocks, merchandisings, quizzes, comments = [], [], [], [], []

        for v in visits:
            vid = v.get("visit_headers", [{}])[0].get("visit_id")
            for h in v.get("visit_headers", []):
                visit_headers.append(h)

            for s in v.get("stocks", []):
                s["visit_id"] = vid
                stocks.append(s)

            for m in v.get("merchandisings", []):
                m["visit_id"] = vid
                merchandisings.append(m)

            for q in v.get("quizzes", []):
                q["visit_id"] = vid
                quizzes.append(q)

            for c in v.get("comments", []):
                c["visit_id"] = vid
                comments.append(c)

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


def clean_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame dagi datetime ustunlarni tozalab datetime64 ga o'tkazish"""
    for col in df.columns:
        col_lower = col.lower()
        if "date" in col_lower or "time" in col_lower or col_lower.endswith("_at"):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            except Exception:
                pass
    return df


def map_sql_types(df: pd.DataFrame):
    """Pandas dtypes + column name pattern -> SQLAlchemy types"""
    type_map = {}
    for col in df.columns:
        series = df[col].dropna()
        col_lower = col.lower()

        if col_lower.endswith("id") or col_lower == "id":
            type_map[col] = Integer
            continue
        if "date" in col_lower or "time" in col_lower or col_lower.endswith("_at"):
            type_map[col] = DateTime
            continue

        if pd.api.types.is_integer_dtype(series):
            type_map[col] = Integer
        elif pd.api.types.is_float_dtype(series):
            type_map[col] = Float
        elif pd.api.types.is_bool_dtype(series):
            type_map[col] = Boolean
        elif pd.api.types.is_datetime64_any_dtype(series):
            type_map[col] = DateTime
        else:
            try:
                max_len = int(series.astype(str).str.len().max())
            except Exception:
                max_len = 255
            type_map[col] = String(length=min(max_len, 1000))
    return type_map


def upload_to_sql(df_dict):
    try:
        print("🔌 Подключение к SQL Server...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=WIN-LORQJU2719N;"
            "DATABASE=SmartUp;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        with engine.begin() as conn:
            for table_name, df in df_dict.items():
                # 🧹 datetime ustunlarni tozalash
                df = clean_datetime_columns(df)

                if "visit_id" in df.columns:
                    df = df.drop_duplicates(subset=["visit_id"])

                column_types = map_sql_types(df)

                if engine.dialect.has_table(conn, table_name):
                    if "visit_id" in df.columns:
                        existing_ids = pd.read_sql(
                            text(f"SELECT DISTINCT visit_id FROM {table_name}"), conn
                        )
                        new_df = df[~df["visit_id"].isin(existing_ids["visit_id"])]
                    else:
                        new_df = df

                    if not new_df.empty:
                        print(f"📥 {table_name} ga {len(new_df)} ta yangi qator qo‘shilmoqda...")
                        new_df.to_sql(table_name, con=conn, index=False, if_exists="append", dtype=column_types)
                    else:
                        print(f"⚡ {table_name} da yangi ma'lumot yo‘q, o‘tkazib yuborildi.")
                else:
                    print(f"🆕 {table_name} jadvali yo‘q, yangisini yaratamiz...")
                    df.to_sql(table_name, con=conn, index=False, if_exists="replace", dtype=column_types)

        print("✅ Все данные успешно записаны в SQL Server (to‘g‘ri turlar bilan, dublikatlarsiz).")

    except Exception as e:
        print(f"❌ Ошибка при записи в SQL: {e}")


if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    DATA_URL = f"https://smartup.online/b/trade/txs/tvt/visit$export?from=2025-01-01&to={today}"
    df_dict = fetch_and_flatten(DATA_URL)
    if df_dict:
        upload_to_sql(df_dict)
