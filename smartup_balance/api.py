# -*- coding: utf-8 -*-
import json
import platform
import sys
from datetime import datetime, timedelta

import pyodbc
import requests

print(sys.getdefaultencoding())  # utf-8 bo'lishi kerak

# ====== KONFIG ======
URL = "https://smartup.online/b/anor/mxsx/mkw/balance$export"
USERNAME = "powerbi@epco"
PASSWORD = "said_2021"
DATE_FORMAT = "%d.%m.%Y"

SQL_SERVER = "WIN-LORQJU2719N"
SQL_DATABASE = "SmartUp"
SQL_TRUSTED = "Yes"  # Windows auth

BEGIN_DATE_STR = "15.02.2025"
END_DATE_STR = "15.04.2025"
FILIAL_WAREHOUSE_JSON = "filial_warehouse.json"

TABLE_NAME = "dbo.BalanceData"
TEXT_COLUMNS = [
    "inventory_kind", "warehouse_code", "product_code", "product_barcode",
    "product_id", "card_code", "measure_code", "filial_code",
    "group_code", "type_code", "serial_number", "batch_number"
]
COLLATION = "Cyrillic_General_CI_AS"


# ====== UTIL ======
def daterange(start_date: datetime, end_date: datetime, step_days: int = 30):
    current = start_date
    while current <= end_date:
        next_date = min(current + timedelta(days=step_days - 1), end_date)
        yield current, next_date
        current = next_date + timedelta(days=1)


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
    driver, all_drivers = _pick_driver()
    if not driver:
        arch = platform.architecture()[0]
        raise RuntimeError(
            f"ODBC drayver topilmadi. Python: {arch}. O'rnatilganlar: {all_drivers}\n"
            "Iltimos, 'ODBC Driver 17/18 for SQL Server' ni o'rnating."
        )

    print(f"➡️  Using ODBC driver: {{{driver}}}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection={SQL_TRUSTED};"
        "Encrypt=No;"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)

    # Unicode
    conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")
    conn.setencoding(encoding="utf-16le")
    return conn


def to_date(val):
    """Matn/iso datetime -> date (yaroqsiz bo'lsa None)."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    # ISO 8601 datetime bo'lsa (YYYY-MM-DDTHH:MM:SS...)
    try:
        return datetime.fromisoformat(s[:19]).date()
    except Exception:
        return None


def to_float(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def table_exists(cursor, table_name: str) -> bool:
    cursor.execute("""
        SELECT 1
        FROM sys.objects
        WHERE object_id = OBJECT_ID(?) AND type IN (N'U')
    """, (table_name,))
    return cursor.fetchone() is not None


def ensure_table_and_columns(cursor):
    """Jadval bo'lmasa yaratadi. Bor bo'lsa kerakli kolonkalarni NVARCHAR + COLLATION ga keltiradi, yo'q bo'lsa qo'shadi."""
    if not table_exists(cursor, TABLE_NAME):
        cursor.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            inventory_kind NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            [date]         DATE NULL,
            warehouse_id   INT NULL,
            warehouse_code NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            product_code   NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            product_barcode NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            product_id     NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            card_code      NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            expiry_date    DATE NULL,
            serial_number  NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            batch_number   NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            quantity       FLOAT NULL,
            measure_code   NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            input_price    FLOAT NULL,
            filial_id      INT NULL,
            filial_code    NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            group_code     NVARCHAR(MAX) COLLATE {COLLATION} NULL,
            type_code      NVARCHAR(MAX) COLLATE {COLLATION} NULL
        );
        """)
        print(f"🆕 Jadval yaratildi: {TABLE_NAME}")
        return

    # Bor jadval: mavjud ustunlarni tekshirish, yo'q bo'lsa qo'shish, noto'g'ri turlarni tuzatish
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
        "group_code": f"NVARCHAR(MAX) COLLATE {COLLATION}",
        "type_code": f"NVARCHAR(MAX) COLLATE {COLLATION}",
    }

    # mavjud ustunlar
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, COLLATION_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = PARSENAME(?,1) OR TABLE_NAME = PARSENAME(?,2)
    """, (TABLE_NAME, TABLE_NAME))
    current = {row[0].lower(): (row[1].lower(), row[2]) for row in cursor.fetchall()}

    for col, dtype in desired_cols.items():
        if col not in current:
            # ustun yo'q -> qo'shamiz
            cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD {col} {dtype} NULL;")
            print(f"🧩 Ustun qo'shildi: {col} {dtype}")
        else:
            cur_type, cur_coll = current[col]
            # type/collationni tekshiruv: faqat NVARCHAR maydonlarida COLLATION qo'llanadi
            if "nvarchar" in dtype.lower() and cur_type != "nvarchar":
                cursor.execute(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN {col} {dtype};")
                print(f"⚠️ {col}: {cur_type} → {dtype} (ALTER)")
            elif "nvarchar" in dtype.lower() and cur_coll != COLLATION:
                cursor.execute(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN {col} {dtype};")
                print(f"⚠️ {col}: COLLATION {cur_coll} → {COLLATION} (ALTER)")
            elif dtype.lower() == "date" and cur_type != "date":
                cursor.execute(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN {col} DATE NULL;")
                print(f"⚠️ {col}: {cur_type} → DATE (ALTER)")
            elif dtype.lower() == "float" and cur_type != "float":
                cursor.execute(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN {col} FLOAT NULL;")
                print(f"⚠️ {col}: {cur_type} → FLOAT (ALTER)")
            elif dtype.lower() == "int" and cur_type != "int":
                cursor.execute(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN {col} INT NULL;")
                print(f"⚠️ {col}: {cur_type} → INT (ALTER)")


def fetch_balance_chunks(filial_warehouse_list, begin_date: datetime, end_date: datetime):
    """API dan ma'lumotlarni olib keladi, duplicatelarni chiqarib tashlaydi va tayyor tuple qaytaradi."""
    session = requests.Session()
    final_rows = []
    seen = set()

    for entry in filial_warehouse_list:
        filial_id = entry["filial_id"]
        filial_code = entry["filial_code"]
        warehouse_id = entry["warehouse_id"]
        warehouse_code = entry["warehouse_code"]

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
                balance = data.get("balance", [])

                added_count = 0
                for item in balance:
                    # Enrichment
                    item["filial_id"] = filial_id
                    item["filial_code"] = filial_code
                    item["warehouse_id"] = warehouse_id
                    item["warehouse_code"] = warehouse_code

                    # De-dup
                    key = json.dumps(item, sort_keys=True, ensure_ascii=False)
                    if key in seen:
                        continue
                    seen.add(key)

                    groups = item.get("groups", []) or [{"group_code": None, "type_code": None}]
                    for g in groups:
                        final_rows.append((
                            item.get("inventory_kind"),
                            to_date(item.get("date")),  # <- DATE ga
                            int(item["warehouse_id"]) if item.get("warehouse_id") else None,
                            item.get("warehouse_code"),
                            item.get("product_code"),
                            item.get("product_barcode"),
                            item.get("product_id"),
                            item.get("card_code"),
                            to_date(item.get("expiry_date")),  # <- DATE ga
                            item.get("serial_number"),
                            item.get("batch_number"),
                            to_float(item.get("quantity")),  # <- FLOAT ga
                            item.get("measure_code"),
                            to_float(item.get("input_price")),  # <- FLOAT ga
                            int(item["filial_id"]) if item.get("filial_id") else None,
                            item.get("filial_code"),
                            g.get("group_code"),
                            g.get("type_code"),
                        ))
                        added_count += 1

                print(f"✅ {start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | "
                      f"filial={filial_code} | warehouse={warehouse_code} | "
                      f"{len(balance)} items ({added_count} new)")

            except Exception as e:
                print(f"⚠️ API xatosi | filial={filial_code} | warehouse={warehouse_code} | "
                      f"{start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | {e}")

    return final_rows


# ====== MAIN ======
def main():
    # 1) JSON ni UTF-8 da o‘qiymiz
    with open(FILIAL_WAREHOUSE_JSON, "r", encoding="utf-8") as f:
        filial_warehouse_list = json.load(f)

    begin_date = datetime.strptime(BEGIN_DATE_STR, DATE_FORMAT)
    end_date = datetime.strptime(END_DATE_STR, DATE_FORMAT)

    # 2) SQL ga ulanib, jadval/kolonkalarni tekshiramiz
    conn = connect_sql()
    cursor = conn.cursor()
    print("✅ SQL Serverga ulandik")

    ensure_table_and_columns(cursor)
    conn.commit()

    # 3) API dan ma’lumotlarni yig‘amiz
    final_rows = fetch_balance_chunks(filial_warehouse_list, begin_date, end_date)
    if not final_rows:
        print("ℹ️ Yangi yozuvlar topilmadi.")
        cursor.close()
        conn.close()
        return

    # 4) Temp jadval yaratamiz
    cursor.execute(f"""
    IF OBJECT_ID('tempdb..#TempBalanceData') IS NOT NULL DROP TABLE #TempBalanceData;

    CREATE TABLE #TempBalanceData (
        inventory_kind NVARCHAR(MAX) COLLATE {COLLATION},
        [date] DATE,
        warehouse_id INT,
        warehouse_code NVARCHAR(MAX) COLLATE {COLLATION},
        product_code NVARCHAR(MAX) COLLATE {COLLATION},
        product_barcode NVARCHAR(MAX) COLLATE {COLLATION},
        product_id NVARCHAR(MAX) COLLATE {COLLATION},
        card_code NVARCHAR(MAX) COLLATE {COLLATION},
        expiry_date DATE,
        serial_number NVARCHAR(MAX) COLLATE {COLLATION},
        batch_number NVARCHAR(MAX) COLLATE {COLLATION},
        quantity FLOAT,
        measure_code NVARCHAR(MAX) COLLATE {COLLATION},
        input_price FLOAT,
        filial_id INT,
        filial_code NVARCHAR(MAX) COLLATE {COLLATION},
        group_code NVARCHAR(MAX) COLLATE {COLLATION},
        type_code NVARCHAR(MAX) COLLATE {COLLATION}
    );
    """)

    # 5) Bulk insert (Unicode safe) — muammo bo'lsa fallback qilamiz
    try:
        cursor.fast_executemany = True
        cursor.executemany("""
            INSERT INTO #TempBalanceData VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, final_rows)
    except pyodbc.Error as e:
        print(f"⚠️ fast_executemany bilan muammo: {e}. Fallback qilib, oddiy executemany bilan davom etamiz.")
        cursor.fast_executemany = False
        cursor.executemany("""
            INSERT INTO #TempBalanceData VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, final_rows)

    # 6) MERGE (faqat yo'q bo'lganlarini qo'shamiz)
    cursor.execute(f"""
    MERGE {TABLE_NAME} AS target
    USING #TempBalanceData AS source
      ON target.warehouse_id = source.warehouse_id
     AND target.product_code  = source.product_code
     AND target.[date]        = source.[date]
     AND ISNULL(target.batch_number, N'') = ISNULL(source.batch_number, N'')
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        inventory_kind, [date], warehouse_id, warehouse_code,
        product_code, product_barcode, product_id, card_code,
        expiry_date, serial_number, batch_number, quantity,
        measure_code, input_price, filial_id, filial_code,
        group_code, type_code
      )
      VALUES (
        source.inventory_kind, source.[date], source.warehouse_id, source.warehouse_code,
        source.product_code, source.product_barcode, source.product_id, source.card_code,
        source.expiry_date, source.serial_number, source.batch_number, source.quantity,
        source.measure_code, source.input_price, source.filial_id, source.filial_code,
        source.group_code, source.type_code
      );
    """)

    # 7) Tozalash va commit
    cursor.execute("DROP TABLE #TempBalanceData;")
    conn.commit()
    cursor.close()
    conn.close()

    print(f"💾 Yuklash yakunlandi | Yozuvlar: {len(final_rows)}")


if __name__ == "__main__":
    main()
