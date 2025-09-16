import urllib

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, String, Boolean, DateTime


def get_cookies_from_browser(url):
    """Открывает браузер и получает cookies после ручного входа"""
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Зайдите на сайт и нажмите Enter после авторизации...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


def auto_cast_dataframe(df: pd.DataFrame) -> (pd.DataFrame, dict):
    """Автоматически приводит типы и возвращает mapping для SQLAlchemy"""
    dtype_mapping = {}

    for col in df.columns:
        series = df[col].dropna()

        if series.empty:
            dtype_mapping[col] = String(255)
            continue

        sample = series.iloc[0]

        if pd.api.types.is_integer_dtype(series):
            df[col] = df[col].astype("Int64")
            dtype_mapping[col] = Integer()
        elif pd.api.types.is_float_dtype(series):
            df[col] = df[col].astype(float)
            dtype_mapping[col] = Float()
        elif pd.api.types.is_bool_dtype(series):
            df[col] = df[col].astype(bool)
            dtype_mapping[col] = Boolean()
        elif pd.api.types.is_datetime64_any_dtype(series):
            df[col] = pd.to_datetime(df[col], errors="coerce")
            dtype_mapping[col] = DateTime()
        else:
            df[col] = df[col].astype(str)
            max_len = min(1000, df[col].str.len().max()) if not df[col].empty else 255
            dtype_mapping[col] = String(int(max_len))

    return df, dtype_mapping


def fetch_and_flatten(data_url):
    try:
        cookies = get_cookies_from_browser("https://smartup.online")
        print("⬇️ Загружаем данные...")
        response = requests.get(data_url, cookies=cookies)
        response.raise_for_status()
        data = response.json()

        # Найти список
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    data = value
                    break
            else:
                raise ValueError("❌ Не найден список в структуре JSON")
        elif not isinstance(data, list):
            raise ValueError("❌ Формат ответа неизвестен")

        # Основная таблица
        return_df = pd.json_normalize(data, sep="_", max_level=1)

        # Подтаблица: return_products
        return_products_list = []
        for entry in data:
            visit_id = entry.get("deal_id") or entry.get("visit_id")
            for product in entry.get("return_products", []):
                product["visit_id"] = visit_id
                return_products_list.append(product)
        return_products_df = pd.DataFrame(return_products_list)

        # Подтаблица: details
        details_list = []
        for product in return_products_list:
            product_id = product.get("product_unit_id")
            visit_id = product.get("visit_id")
            for detail in product.get("details", []):
                detail["product_id"] = product_id
                detail["visit_id"] = visit_id
                details_list.append(detail)
        details_df = pd.DataFrame(details_list)

        print(f"✅ Получено: {len(return_df)} возвратов, {len(return_products_df)} товаров, {len(details_df)} деталей")

        df_dict = {
            name: df for name, df in {
                "visit_return": return_df,
                "visit_returnproducts": return_products_df,
                "visit_details": details_df
            }.items() if not df.empty and not df.columns.empty
        }

        return df_dict

    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")
        return None


def upload_to_sql(df_dict):
    try:
        print("🔌 Подключение к SQL Server...")
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=TAKEDA;"
            "DATABASE=DealDB;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        for table_name, df in df_dict.items():
            print(f"📥 Подготовка таблицы: {table_name} ({len(df)} строк)")

            # Убираем дубликаты
            df = df.drop_duplicates()

            # Автоматический каст и mapping типов
            df, dtype_mapping = auto_cast_dataframe(df)

            # Запись
            df.to_sql(table_name, con=engine, index=False, if_exists="replace", dtype=dtype_mapping)

            print(f"✅ {table_name} загружена ({len(df)} строк)")

        print("🎉 Все данные успешно записаны в SQL Server.")

    except Exception as e:
        print(f"❌ Ошибка при записи в SQL: {e}")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/trade/txs/tvt/visit$export"
    df_dict = fetch_and_flatten(DATA_URL)
    if df_dict:
        upload_to_sql(df_dict)
