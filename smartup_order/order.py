import urllib
import json
import pandas as pd
import requests
from sqlalchemy import create_engine


USERNAME = "powerbi@epco"
PASSWORD = "said_2021"
DATA_URL = "https://smartup.online/b/trade/txs/tdeal/order$export"


def load_filials(filepath="order.json"):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data  # list qaytadi


def fetch_and_flatten(data_url, filial_id):
    try:
        # Auth bilan POST request
        response = requests.post(
            data_url,
            auth=(USERNAME, PASSWORD),  # Basic Auth
            headers={"project_code": "trade"},
            json={"filial_id": filial_id}
        )
        response.raise_for_status()
        data = response.json()

        orders = data.get("order", [])
        if not orders:
            print(f"⚠️ Filial {filial_id} bo‘yicha 'order' bo‘sh")
            return None

        # Asosiy order jadvali
        order_df = pd.json_normalize(orders, sep="_", max_level=1)
        order_df["EXTERNAL_FILIAL"] = filial_id

        # order_products jadvali
        order_products_list = []
        for order in orders:
            order_id = order.get("deal_id")
            for product in order.get("order_products", []):
                product["order_id"] = order_id
                product["EXTERNAL_FILIAL"] = filial_id
                order_products_list.append(product)
        order_products_df = pd.DataFrame(order_products_list)

        # details jadvali
        details_list = []
        for product in order_products_list:
            product_id = product.get("product_id")
            order_id = product.get("order_id")
            for detail in product.get("details", []):
                detail["product_id"] = product_id
                detail["order_id"] = order_id
                detail["EXTERNAL_FILIAL"] = filial_id
                details_list.append(detail)
        details_df = pd.DataFrame(details_list)

        return {
            "order_main": order_df,
            "order_products": order_products_df,
            "order_details": details_df
        }
    except Exception as e:
        print(f"❌ Xatolik (filial {filial_id}): {e}")
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
            if df is None or df.empty:
                continue
            df.to_sql(table_name, con=engine, index=False, if_exists="append")
            print(f"✅ {table_name} jadvaliga {len(df)} satr yozildi.")
    except Exception as e:
        print(f"❌ SQL yozishda xatolik: {e}")


if __name__ == "__main__":
    filial_list = load_filials("order.json")

    for filial in filial_list:
        filial_id = filial["filial_id"]
        print(f"⬇️ Yuklanmoqda filial_id={filial_id} ...")
        df_dict = fetch_and_flatten(DATA_URL, filial_id)
        if df_dict:
            upload_to_sql(df_dict)
