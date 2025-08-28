import json
import platform
import pyodbc
import requests
from datetime import datetime, timedelta, date

# ====== UTIL ======
def today_samarkand() -> date:
    """Asia/Samarkand ~ UTC+5 (без pytz)"""
    return (datetime.utcnow() + timedelta(hours=5)).date()


def daterange(start_date: date, end_date: date, step_days: int = 30):
    current = start_date
    while current <= end_date:
        next_date = min(current + timedelta(days=step_days - 1), end_date)
        yield current, next_date
        current = next_date + timedelta(days=1)


def to_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def to_float(val):
    try:
        return float(val)
    except Exception:
        return None


def _pick_driver():
    drivers = [d.strip() for d in pyodbc.drivers()]
    for name in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]:
        if name in drivers:
            return name, drivers
    return None, drivers


def connect_sql():
    driver = "{ODBC Driver 18 for SQL Server}"
    server = "WIN-LORQJU2719N"
    database = "SmartUp"

    print(f"➡️  Используется ODBC driver: {{{driver}}}")

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=Yes;"
        "Encrypt=No;"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)

    # Unicode
    conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")
    conn.setencoding(encoding="utf-16le")
    return conn


# ====== CONFIG ======
URL = "https://smartup.online/b/anor/mxsx/mkw/balance$export"
USERNAME = "powerbi@epco"
PASSWORD = "said_2021"
DATE_FORMAT = "%d.%m.%Y"

SQL_SERVER = "WIN-LORQJU2719N"
SQL_DATABASE = "SmartUp"
SQL_TRUSTED = "Yes"

BEGIN_DATE_FIXED = date(2025, 1, 1)

FILIAL_WAREHOUSE_JSON = "filial_warehouse1.json"
TABLE_NAME = "dbo.BalanceData"

# NVARCHAR(MAX) поля
TEXT_COLUMNS = [
    "inventory_kind", "warehouse_code", "product_code", "product_barcode",
    "product_id", "card_code", "measure_code", "filial_code",
    "serial_number", "batch_number",
    "group_name", "category_name", "brand_name"
]
COLLATION = "Cyrillic_General_CI_AS"


# ====== FETCH API ======
def fetch_balance_chunks(filial_warehouse_list, begin_date: date, end_date: date):
    """
    Загружает данные из API, убирает дубликаты и возвращает список tuple для SQL.
    """
    session = requests.Session()
    final_rows = []
    seen = set()

    for entry in filial_warehouse_list:
        filial_id = entry.get("filial_id")
        filial_code = entry.get("filial_code")
        warehouse_id = entry.get("warehouse_id")
        warehouse_code = entry.get("warehouse_code")

        for start, finish in daterange(begin_date, end_date, step_days=30):
            params = {"filial_id": filial_id}
            payload = {
                "warehouse_codes": [{"warehouse_code": warehouse_code}],
                "filial_code": filial_code,
                "begin_date": start.strftime(DATE_FORMAT),
                "end_date": finish.strftime(DATE_FORMAT)
            }

            try:
                resp = session.post(
                    URL,
                    params=params,
                    auth=(USERNAME, PASSWORD),
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    timeout=60,
                )
                resp.encoding = "utf-8"
                resp.raise_for_status()
                data = resp.json()
                balance = data.get("balance", []) or []

                added_count = 0
                for item in balance:
                    # enrich
                    item["filial_id"] = filial_id
                    item["filial_code"] = filial_code
                    item["warehouse_id"] = warehouse_id
                    item["warehouse_code"] = warehouse_code

                    # dedup
                    key = json.dumps(item, sort_keys=True, ensure_ascii=False)
                    if key in seen:
                        continue
                    seen.add(key)

                    # --- новая схема: group/category/brand ---
                    group_name, category_name, brand_name = None, None, None
                    groups = item.get("groups", [])
                    if groups and isinstance(groups, list):
                        for g in groups:
                            if g.get("type") == "Group":
                                group_name = g.get("name")
                            elif g.get("type") == "Category":
                                category_name = g.get("name")
                            elif g.get("type") == "Brand":
                                brand_name = g.get("name")

                    final_rows.append((
                        item.get("inventory_kind"),
                        to_date(item.get("date")),
                        int(item["warehouse_id"]) if item.get("warehouse_id") else None,
                        item.get("warehouse_code"),
                        item.get("product_code"),
                        item.get("product_barcode"),
                        item.get("product_id"),
                        item.get("card_code"),
                        to_date(item.get("expiry_date")),
                        item.get("serial_number"),
                        item.get("batch_number"),
                        to_float(item.get("quantity")),
                        item.get("measure_code"),
                        to_float(item.get("input_price")),
                        int(item["filial_id"]) if item.get("filial_id") else None,
                        item.get("filial_code"),
                        group_name,
                        category_name,
                        brand_name
                    ))
                    added_count += 1

                print(f"✅ {start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | "
                      f"filial={filial_code} | warehouse={warehouse_code} | "
                      f"{len(balance)} items ({added_count} new)")

            except Exception as e:
                print(f"⚠️ Ошибка API | filial={filial_code} | warehouse={warehouse_code} | "
                      f"{start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | {e}")

    return final_rows


# ====== SQL LOADER ======
def load_to_sql(rows):
    if not rows:
        print("❌ Нет данных для загрузки")
        return

    conn = connect_sql()
    cur = conn.cursor()

    desired_cols = {
        "inventory_kind": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "date": "DATE",
        "warehouse_id": "INT",
        "warehouse_code": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "product_code": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "product_barcode": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "product_id": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "card_code": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "expiry_date": "DATE",
        "serial_number": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "batch_number": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "quantity": "FLOAT",
        "measure_code": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "input_price": "FLOAT",
        "filial_id": "INT",
        "filial_code": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "group_name": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "category_name": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "brand_name": f"NVARCHAR(MAX) COLLATE {COLLATION}",
    }

    # создать временную таблицу
    cur.execute("IF OBJECT_ID('tempdb..#TempBalanceData') IS NOT NULL DROP TABLE #TempBalanceData;")
    columns_sql = ", ".join([f"{c} {t}" for c, t in desired_cols.items()])
    cur.execute(f"CREATE TABLE #TempBalanceData ({columns_sql});")

    # bulk insert
    placeholders = ",".join("?" * len(desired_cols))
    insert_sql = f"INSERT INTO #TempBalanceData VALUES ({placeholders})"
    cur.executemany(insert_sql, rows)
    print(f"📥 Загружено {len(rows)} строк во временную таблицу")

    # MERGE
    merge_sql = f"""
    MERGE {TABLE_NAME} AS target
    USING #TempBalanceData AS src
    ON target.date = src.date
       AND target.product_code = src.product_code
       AND target.warehouse_code = src.warehouse_code
    WHEN MATCHED THEN
        UPDATE SET
            target.inventory_kind = src.inventory_kind,
            target.product_barcode = src.product_barcode,
            target.product_id = src.product_id,
            target.card_code = src.card_code,
            target.expiry_date = src.expiry_date,
            target.serial_number = src.serial_number,
            target.batch_number = src.batch_number,
            target.quantity = src.quantity,
            target.measure_code = src.measure_code,
            target.input_price = src.input_price,
            target.filial_id = src.filial_id,
            target.filial_code = src.filial_code,
            target.group_name = src.group_name,
            target.category_name = src.category_name,
            target.brand_name = src.brand_name
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({", ".join(desired_cols.keys())})
        VALUES ({", ".join([f"src.{c}" for c in desired_cols.keys()])});
    """
    cur.execute(merge_sql)
    print("🔄 MERGE завершен")

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Данные успешно загружены")


# ====== MAIN ======
def main():
    # загрузка конфигурации filial_warehouse.json
    with open(FILIAL_WAREHOUSE_JSON, "r", encoding="utf-8") as f:
        filial_warehouse_list = json.load(f)

    begin_date = BEGIN_DATE_FIXED
    end_date = today_samarkand()

    rows = fetch_balance_chunks(filial_warehouse_list, begin_date, end_date)
    load_to_sql(rows)


if __name__ == "__main__":
    main()
